export function computeCapacity(ramGB: number, perInstanceGB = 4): number {
  if (!Number.isFinite(ramGB) || ramGB <= 0) return 0;
  if (!Number.isFinite(perInstanceGB) || perInstanceGB <= 0) return 0;
  return Math.floor(ramGB / perInstanceGB);
}

