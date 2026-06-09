const TIER_TECH_IDS = [200, 300, 400, 500, 600, 700, 800, 900, 1000];

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
