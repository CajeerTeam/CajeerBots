#!/usr/bin/env node
// CycloneDX 1.5 inventory from the frozen installed workspace tree and actual image OS package database.
import { readFile, writeFile } from 'node:fs/promises';
import { resolve, relative, isAbsolute, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import { randomUUID } from 'node:crypto';
import { validVersion } from './release-artifacts.mjs';
const npmName = /^(?:@[a-z0-9._-]+\/)?[a-z0-9._-]+$/i;
const componentVersion = /^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:[-+][0-9A-Za-z.+-]+)?$/;
const npmPurl = (name, version) => `pkg:npm/${name.split('/').map(encodeURIComponent).join('/')}@${encodeURIComponent(version)}`;
export async function buildSbom({ tree, osPackages, nodeVersion, version, root }) {
  if (!Array.isArray(tree) || !tree.length || !validVersion(version) || !componentVersion.test(nodeVersion)) throw new Error('Invalid SBOM inputs');
  const appRef = npmPurl('cajeerbots', version);
  const components = new Map(), edges = new Map();
  const edge = (from,to) => { if (from === to) return; const set = edges.get(from) ?? new Set(); set.add(to); edges.set(from,set); };
  const add = (component) => { components.set(component['bom-ref'],component); if (!edges.has(component['bom-ref'])) edges.set(component['bom-ref'],new Set()); return component['bom-ref']; };
  edges.set(appRef,new Set());
  async function visit(entry, fallbackName, parent, depth=0) {
    if (depth > 256 || components.size > 100000) throw new Error('Dependency graph exceeds limit');
    let name = entry.name ?? fallbackName, ver = entry.version;
    if (!componentVersion.test(ver ?? '')) {
      // pnpm lists workspace links by link: path. Resolve actual installed package metadata.
      if (typeof entry.path !== 'string') throw new Error(`Missing installed version: ${name}`);
      const path = resolve(entry.path); const rel = relative(root,path);
      if (rel.startsWith('..') || isAbsolute(rel)) throw new Error('Package outside workspace');
      const pkg = JSON.parse(await readFile(join(path,'package.json'),'utf8')); name = pkg.name; ver = pkg.version;
    }
    if (!npmName.test(name ?? '') || !componentVersion.test(ver ?? '')) throw new Error(`Invalid installed package identity: ${name}`);
    const ref = npmPurl(name,ver);
    if(ref !== appRef) add({type:'library',name,version:ver,purl:ref,'bom-ref':ref});
    edge(parent,ref);
    for (const key of ['dependencies','devDependencies','optionalDependencies','peerDependencies']) {
      for (const [child, value] of Object.entries(entry[key] ?? {})) {
        if (value && typeof value === 'object') await visit(value,child,ref,depth+1);
        // string-valued peer ranges are requirements, not installed dependencies.
      }
    }
  }
  for (const item of tree) await visit(item,item.name,appRef);
  if (components.size === 0) throw new Error('Empty installed dependency inventory');
  const nodeRef = `pkg:generic/nodejs@${encodeURIComponent(nodeVersion)}`;
  edge(appRef,add({type:'framework',name:'nodejs',version:nodeVersion,purl:nodeRef,'bom-ref':nodeRef}));
  let osCount=0;
  for(const line of osPackages.trim().split('\n')) {
    const [name, ver, arch] = line.split('\t');
    if(!name || !ver || !arch || !/^[a-z0-9.+-]+(?::[a-z0-9]+)?$/.test(name) || !/^[a-z0-9]+$/.test(arch)) throw new Error('Invalid dpkg inventory');
    const ref=`pkg:deb/debian/${encodeURIComponent(name.split(':')[0])}@${encodeURIComponent(ver)}?arch=${arch}`;
    edge(appRef,add({type:'library',name,version:ver,purl:ref,'bom-ref':ref})); osCount++;
  }
  if(!osCount) throw new Error('Empty OS inventory');
  return {bomFormat:'CycloneDX',specVersion:'1.5',serialNumber:`urn:uuid:${randomUUID()}`,version:1,metadata:{timestamp:new Date().toISOString(),component:{type:'application',name:'cajeerbots',version,purl:appRef,'bom-ref':appRef}},components:[...components.values()].sort((a,b)=>a['bom-ref'].localeCompare(b['bom-ref'])),dependencies:[...edges.entries()].sort(([a],[b])=>a.localeCompare(b)).map(([ref,set])=>({ref,dependsOn:[...set].sort()}))};
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  const [treeFile,osFile,nodeVersion,out] = process.argv.slice(2);
  if(!treeFile||!osFile||!nodeVersion||!out) throw new Error('Usage: release-sbom.mjs PNPM_TREE.json DPKG.tsv NODE_VERSION OUTPUT.json');
  const version=(await readFile('VERSION','utf8')).trim();
  const bom=await buildSbom({tree:JSON.parse(await readFile(treeFile,'utf8')),osPackages:await readFile(osFile,'utf8'),nodeVersion:nodeVersion.replace(/^v/,''),version,root:process.cwd()});
  await writeFile(out,JSON.stringify(bom,null,2)+'\n',{flag:'wx'});
}
