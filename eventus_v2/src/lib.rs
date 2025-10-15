use ahash::RandomState;

pub mod bucket;
pub mod id;

const SEED_K0: u64 = 0x53c8ff368077e723;
const SEED_K1: u64 = 0x586c670340740e26;
const SEED_K2: u64 = 0x34c309d85840faf5;
const SEED_K3: u64 = 0x392f381a75a0be2b;
const RANDOM_STATE: RandomState = RandomState::with_seeds(SEED_K0, SEED_K1, SEED_K2, SEED_K3);
