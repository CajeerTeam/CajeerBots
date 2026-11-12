-- Opt-in database capacity enforcement. No row means unrestricted deployment.
CREATE TABLE "pilot_capacity" (
  "id" INTEGER PRIMARY KEY CHECK ("id" = 1),
  "maxCommunities" INTEGER NOT NULL CHECK ("maxCommunities" BETWEEN 1 AND 5),
  "maxBotAccounts" INTEGER NOT NULL CHECK ("maxBotAccounts" BETWEEN 1 AND 10),
  "communities" INTEGER NOT NULL CHECK ("communities" >= 0 AND "communities" <= "maxCommunities"),
  "botAccounts" INTEGER NOT NULL CHECK ("botAccounts" >= 0 AND "botAccounts" <= "maxBotAccounts")
);
-- Row updates serialize capacity changes and roll back with the business write.
CREATE FUNCTION cajeer_pilot_capacity_change() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE delta integer;
BEGIN
  delta := CASE WHEN TG_OP = 'INSERT' THEN 1 ELSE -1 END;
  IF TG_TABLE_NAME = 'communities' THEN
    UPDATE public.pilot_capacity SET "communities" = "communities" + delta WHERE id = 1;
  ELSIF TG_TABLE_NAME = 'bot_accounts' THEN
    UPDATE public.pilot_capacity SET "botAccounts" = "botAccounts" + delta WHERE id = 1;
  END IF;
  RETURN NULL;
END;
$$;
CREATE TRIGGER cajeer_pilot_community_capacity AFTER INSERT OR DELETE ON "communities"
FOR EACH ROW EXECUTE FUNCTION cajeer_pilot_capacity_change();
CREATE TRIGGER cajeer_pilot_bot_capacity AFTER INSERT OR DELETE ON "bot_accounts"
FOR EACH ROW EXECUTE FUNCTION cajeer_pilot_capacity_change();
