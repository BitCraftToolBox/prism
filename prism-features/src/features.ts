import {ClaimLocalStateData, ClaimStateData, ClaimTechStateData, get_some_location, GlobalData, RegionData,} from "./types";
import {compute_claim_tier, format_template_args} from "./utils";
import {make_tower_feature, WatchtowerTerritory} from "./watchtower";

const caveTypes = [790011334, 1845065396, 280863630, 696858550, 1440765680, 312420794, 1875067311, 253216585, 1477951340];

export interface OutputData {
    towers: unknown[];
    caves: unknown[];
    trees: unknown[];
    empireResources: any[];
    events: any[];
    npcs: any[];
    temples: unknown[];
    dungeons: unknown[];
    grids: unknown[];
    claims: unknown[];
    gameIcons: unknown[];
}

interface ClaimExtras {
    regional_bank_claim_ids: Set<bigint>;
    bank_claim_ids: Set<bigint>;
    market_claim_ids: Set<bigint>;
    waystone_claim_ids: Set<bigint>;
    claim_tech_map: Map<bigint, ClaimTechStateData>;
    game_icons: Map<number, {title: string; fontIcon: string}>;
}

// Buildings that have a game icon but are already emitted by another feature set:
// hexite deposit, ancient hexite reservoir, maker's tree (empireResources) and hexite vault (events).
const gameIconExcluded = new Set([421789207, 1506763792, 1375306631, 578530093]);

function make_feature(props: Record<string, unknown>, x: number, z: number): unknown {
    return {
        type: "Feature",
        properties: props,
        geometry: {type: "Point", coordinates: [x, z]},
    };
}

export function make_claim_extras(data: RegionData, global_data: GlobalData): ClaimExtras {
    const game_icons = new Map<number, {title: string; fontIcon: string}>();
    for (const row of global_data.building_map_icon_desc) {
        // skips the empty placeholder row and buildings covered elsewhere
        if (!row.iconAddress || gameIconExcluded.has(row.buildingId)) continue;
        game_icons.set(row.buildingId, {title: row.title, fontIcon: row.iconAddress});
    }
    return {
        game_icons,
        regional_bank_claim_ids: new Set(data.regional_bank_state.map((row) => row.claim_entity_id)),
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
        events: [],
        npcs: [],
        temples: [],
        dungeons: [],
        grids: [],
        claims: [],
        gameIcons: [],
    };
}

export function add_feature(
    outputs: OutputData,
    claim_state: ClaimStateData,
    local_state: ClaimLocalStateData,
    territories: WatchtowerTerritory[],
    claim_extras: ClaimExtras,
): void {
    const location = get_some_location(local_state.location);
    if (!location) return;

    const claim_name = format_template_args(claim_state.name);

    const game_icon = claim_extras.game_icons.get(local_state.building_description_id);
    if (game_icon) {
        outputs.gameIcons.push(make_feature({name: game_icon.title, fontIcon: game_icon.fontIcon}, location.x, location.z));
    }

    switch (local_state.building_description_id) {
        case 433549604:
            outputs.trees.push(make_feature({name: claim_name, type: "tree"}, location.x, location.z));
            break;
        case 421789207:
        case 1506763792:
        case 1375306631: {
            const type = local_state.building_description_id === 1375306631 ? 'makers-tree' : 'hexite';
            outputs.empireResources.push(make_feature({
                name: claim_name, type, timer: null, building_desc: local_state.building_description_id
            }, location.x, location.z));
            break;
        }
        case 578530093:
            outputs.events.push(make_feature({
                name: 'Hexite Vault',
                type: 'vault-event',
                timer: null,
                iconName: 'vault-event'
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
        case 1785852446: // giant skitch
        case 208697589: // starseeker armory (sentinel)
        case 1084069097: // araknir den
        case 846734170: // alpha jakyl (small icon)
        case 853770752: // abandoned immortal outpost
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
                        has_regional_bank: claim_extras.regional_bank_claim_ids.has(claim_state.entity_id) ? 1 : 0,
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
