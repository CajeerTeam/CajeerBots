#!/usr/bin/env node
import { Command } from 'commander';
import { installCommand } from './commands/install.command.js';
import { doctorCommand } from './commands/doctor.command.js';
import { generateCommand } from './commands/generate.command.js';
import { extensionCommand } from './commands/extension.command.js';
import { updateCommand } from './commands/update.command.js';
import { authCommand } from './commands/auth.command.js';
import { botCommand } from './commands/bot.command.js';
import { secretsCommand } from './commands/secrets.command.js';
import { configCommand } from './commands/config.command.js';
import { importExportCommand } from './commands/import-export.command.js';
import { interactionsCommand } from './commands/interactions.command.js';
import { runtimeCommand } from './commands/runtime.command.js';
import { messageCoreCommand } from './commands/message-core.command.js';
import { eventApiCommand } from './commands/event-api.command.js';
import { registryCommand } from './commands/registry.command.js';
import { jobsCommand } from './commands/jobs.command.js';
import { devtoolsCommand } from './commands/devtools.command.js';
import { devCommand } from './commands/dev.command.js';
import { backupCommand } from './commands/backup.command.js';
import { storageCommand } from './commands/storage.command.js';
import { observabilityCommand } from './commands/observability.command.js';
import { deploymentCommand } from './commands/deployment.command.js';
import { contractsCommand } from './commands/contracts.command.js';
import { docsCommand } from './commands/docs.command.js';
import { releaseGatesCommand } from './commands/release-gates.command.js';
import { integrationTestsCommand } from './commands/integration-tests.command.js';
import { securityCommand } from './commands/security.command.js';
import { releaseCandidateCommand } from './commands/release-candidate.command.js';

const program = new Command();

program
  .name('cajeer')
  .description('CajeerBots command suite')
  .version('1.0.0-rc.5');

program.addCommand(devCommand());
program.addCommand(installCommand());
program.addCommand(doctorCommand());
program.addCommand(authCommand());
program.addCommand(botCommand());
program.addCommand(secretsCommand());
program.addCommand(configCommand());
program.addCommand(importExportCommand());
program.addCommand(interactionsCommand());
program.addCommand(runtimeCommand());
program.addCommand(messageCoreCommand());
program.addCommand(eventApiCommand());
program.addCommand(registryCommand());
program.addCommand(jobsCommand());
program.addCommand(devtoolsCommand());
program.addCommand(storageCommand());
program.addCommand(backupCommand());
program.addCommand(observabilityCommand());
program.addCommand(deploymentCommand());
program.addCommand(contractsCommand());
program.addCommand(docsCommand());
program.addCommand(releaseGatesCommand());
program.addCommand(integrationTestsCommand());
program.addCommand(securityCommand());
program.addCommand(releaseCandidateCommand());
program.addCommand(generateCommand());
program.addCommand(extensionCommand());
program.addCommand(updateCommand());

await program.parseAsync(process.argv);
