export const SETUP_RECIPE_SERVER_ARGUMENTS: readonly string[] = [
  "--maxmemory-policy",
  "noeviction",
  "--appendonly",
  "yes",
];
