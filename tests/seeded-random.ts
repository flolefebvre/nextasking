export type SeededRandomness = {
  random: () => number;
  pick: <Item>(items: readonly Item[]) => Item;
  seedMessage: string;
};

function seedOfThisRun(): number {
  const seedVariable = process.env.NEXTASKING_TEST_SEED;
  return seedVariable === undefined || seedVariable === "" ? Math.floor(Math.random() * 2 ** 32) : Number(seedVariable);
}

function seededRandom(seed: number): () => number {
  let state = seed >>> 0;
  return () => {
    state = (state + 0x6d2b79f5) >>> 0;
    let mixed = state;
    mixed = Math.imul(mixed ^ (mixed >>> 15), mixed | 1);
    mixed ^= mixed + Math.imul(mixed ^ (mixed >>> 7), mixed | 61);
    return ((mixed ^ (mixed >>> 14)) >>> 0) / 2 ** 32;
  };
}

export function seededRandomnessOfThisRun(): SeededRandomness {
  const seed = seedOfThisRun();
  const random = seededRandom(seed);
  return {
    random,
    pick: <Item>(items: readonly Item[]): Item => items[Math.floor(random() * items.length)] as Item,
    seedMessage: `replay with NEXTASKING_TEST_SEED=${String(seed)}`,
  };
}
