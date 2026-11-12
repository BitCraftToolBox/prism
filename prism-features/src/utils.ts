const TIER_TECH_IDS = [1, 1623246356, 43715291, 443589242, 1461951487, 1510210914, 1756599942, 1976017717, 442389180];

export function format_template_args(value: string): string {
    if (!value.includes("|~")) return value;

    const [template, ...args] = value.split("|~");
    return template.replace(/\{(\d+)}/g, (match, index) => {
        const arg_index = Number(index);
        if (!Number.isInteger(arg_index) || arg_index < 0 || arg_index >= args.length) {
            return match;
        }
        return args[arg_index];
    });
}

export function compute_claim_tier(learned: number[]): number {
    for (let i = TIER_TECH_IDS.length - 1; i >= 0; i--) {
        if (learned.includes(TIER_TECH_IDS[i])) {
            return i + 2;
        }
    }
    return 1;
}
