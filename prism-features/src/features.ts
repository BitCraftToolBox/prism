import {ClaimLocalStateData, ClaimStateData, ClaimTechStateData, get_some_location, GrowthStateTimers, RegionData,} from "./types";
import {compute_claim_tier, format_template_args} from "./utils";
import {make_tower_feature, WatchtowerTerritory} from "./watchtower";

const caveTypes = [790011334, 1845065396, 280863630, 696858550, 1440765680, 312420794, 1875067311, 253216585, 1477951340];

export interface OutputData {
    towers: unknown[];
    caves: unknown[];
    trees: unknown[];
    empireResources: any[];
    uncharted: any[];
    events: any[];
    npcs: any[];
    temples: unknown[];
    dungeons: unknown[];
    grids: unknown[];
    claims: unknown[];
}

interface ClaimExtras {
    bank_claim_ids: Set<bigint>;
    market_claim_ids: Set<bigint>;
    waystone_claim_ids: Set<bigint>;
    claim_tech_map: Map<bigint, ClaimTechStateData>;
}

function make_feature(props: Record<string, unknown>, x: number, z: number): unknown {
    return {
        type: "Feature",
        properties: props,
        geometry: {type: "Point", coordinates: [x, z]},
    };
}

export function make_claim_extras(data: RegionData): ClaimExtras {
    return {
        bank_claim_ids: new Set(data.bank_state.map((row) => row.claim_entity_id)),
        market_claim_ids: new Set(data.marketplace_state.map((row) => row.claim_entity_id)),
        waystone_claim_ids: new Set(data.waystone_state.map((row) => row.claim_entity_id)),
        claim_tech_map: new Map(data.claim_tech_state.map((row) => [row.entity_id, row])),
    };
}

export function create_outputs(): OutputData {
    return {
        towers: [],
        caves: [],
        trees: [],
        empireResources: [],
        uncharted: [],
        events: [],
        npcs: [],
        temples: [],
        dungeons: [],
        grids: [],
        claims: [],
    };
}

export function add_feature(
    outputs: OutputData,
    claim_state: ClaimStateData,
    local_state: ClaimLocalStateData,
    territories: WatchtowerTerritory[],
    growth_timers: GrowthStateTimers[],
    claim_extras: ClaimExtras,
): void {
    const location = get_some_location(local_state.location);
    if (!location) return;

    function findTimer(loc) {
        // find first timer within 5 block radius. none of the things we're interested in tracking should ever be this close
        // i.e., vaults, hexite, maker's trees
        return growth_timers.find(t => Math.pow(t.location.x - loc.x, 2) + Math.pow(t.location.z - loc.z, 2) < 25);
    }

    const claim_name = format_template_args(claim_state.name);

    switch (local_state.building_description_id) {
        case 433549604:
            outputs.trees.push(make_feature({name: claim_name, type: "tree"}, location.x, location.z));
            break;
        case 421789207:
        case 1375306631: {
            const timer = findTimer(location)?.end_timestamp;
            const type = local_state.building_description_id === 421789207 ? 'hexite' : 'makers-tree';
            outputs.empireResources.push(make_feature({name: claim_name, type, timer}, location.x, location.z));
            break;
        }
        case 578530093:
            outputs.events.push(make_feature({
                name: 'Hexite Vault',
                type: 'vault-event',
                timer: findTimer(location)?.end_timestamp,
                iconName: 'vault-event'
            }, location.x, location.z));
            break;
        case 719999256:
            outputs.uncharted.push(make_feature({
                name: claim_name,
                iconName: 'volcanic-geyser'
            }, location.x, location.z));
            break;
        case 1503293649:
            outputs.uncharted.push(make_feature({
                name: claim_name,
                iconName: 'hermit-crab',
                iconSize: [25, 25]
            }, location.x, location.z));
            break;
        case 489406613:
        case 1752479333:
        case 1662809355:
        case 2034914963:
        case 1008368350:
            outputs.temples.push(make_feature({name: claim_name}, location.x, location.z));
            break;
        case 1285450540:
            outputs.npcs.push(make_feature({
                name: claim_name,
                type: 'traveler-camp',
                iconName: 'traveler-camp'
            }, location.x, location.z));
            break;
        case 292245080:
            outputs.npcs.push(make_feature({name: claim_name, type: 'ruined-city'}, location.x, location.z));
            break;
        case 790011334:
        case 280863630:
        case 1875067311:
        case 1845065396:
        case 696858550:
        case 312420794:
        case 253216585:
        case 1477951340:
        case 1440765680:
            outputs.caves.push(
                make_feature(
                    {
                        name: claim_name,
                        size: claim_name.startsWith("Large ") ? 2 : 1,
                        tier: Math.max(1, caveTypes.indexOf(local_state.building_description_id)),
                    },
                    location.x,
                    location.z,
                ),
            );
            break;
        case 1785852446:
        case 208697589:
        case 1084069097:
        case 846734170:
        case 1385919449:
            outputs.dungeons.push(
                make_feature(
                    {
                        popupText: claim_name,
                        iconName: "dungeon",
                        iconSize: local_state.building_description_id === 846734170 ? [25, 25] : [35, 35],
                        type: local_state.building_description_id,
                    },
                    location.x,
                    location.z,
                ),
            );
            break;
        case 90000: {
            const tower = make_tower_feature(claim_state, local_state, territories);
            if (tower) outputs.towers.push(...tower.features);
            break;
        }
        case 405: {
            const tech_state = claim_extras.claim_tech_map.get(claim_state.entity_id);
            const tier = tech_state ? compute_claim_tier(tech_state.learned) : 1;
            outputs.claims.push(
                make_feature(
                    {
                        entityId: String(claim_state.entity_id),
                        name: claim_name,
                        tier,
                        has_bank: claim_extras.bank_claim_ids.has(claim_state.entity_id) ? 1 : 0,
                        has_market: claim_extras.market_claim_ids.has(claim_state.entity_id) ? 1 : 0,
                        has_waystone: claim_extras.waystone_claim_ids.has(claim_state.entity_id) ? 1 : 0,
                    },
                    location.x,
                    location.z,
                ),
            );
            break;
        }
    }
}
