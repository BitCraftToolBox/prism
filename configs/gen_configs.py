import os
import sys

region_groups = {
    # main regions
    (3, "top"): [17, 18, 19],
    (2, "mid"): [12, 13, 14],
    (1, "bot"): [7, 8, 9],
    # temp regions
    (4, "temp"): [3, 11, 15, 23]
}

mapper = [7, 8, 9, 12, 13, 14, 17, 18, 19, 3, 11, 15, 23]

out_dir = sys.argv[1] if len(sys.argv) > 1 else ""

with open("prism.toml.template") as f:
    prism_template = f.read()

with open("compose.yml.template") as f:
    compose_template = f.read()

compose_header, compose_service = compose_template.split("$SERVICE$", 1)

before, after = prism_template.split("$REGIONS$")

offset1 = 0
offset2 = 15

compose_output = compose_header

for (group, name), regions in region_groups.items():
    compose_output += compose_service.replace("$GROUP$", str(group)).replace("$NAME$", "prism-" + (name or str(group)))

    outfile = f"prism-{group}.toml"
    output = before.replace("$OFFSET$", str(offset1)).replace("$OFFSET2$", str(offset2))
    for region in regions:
        output += "\n[[upstream.regions]]\n"\
                  f"name = \"bitcraft-live-{region}\"\n"\
                  f"id = {region}\n"
    output += after
    offset1 += 1
    offset2 += 1
    with open(os.path.join(out_dir, outfile), "w") as f:
        f.write(output)

with open(os.path.join(out_dir, "docker-compose.prism.yml"), "w") as f:
    f.write(compose_output)


m_h = """
[upstream]
host = "https://bitcraft-early-access.spacetimedb.com"
"""
m_p = """
[pipelines]
resources = false
enemies = false
players = false
"""
m_d_1 = """
[[upstream.regions.dumps]]
schedule = "{m_offset_1} 55 3 * * *"
tables = [
    {{ name = "terrain_chunk_state" }},{m_global_1}
]
"""
m_global_1 = '\n    { name = "biome_desc", output_folder = "global" },'
m_d_2 = """
[[upstream.regions.dumps]]
schedule = "{m_offset_2} 45 * * * *"
tables = [
    {{ name = "paved_tile_state" }},
    {{ name = "location_state", output_file = "road_locations", query = "SELECT loc.* FROM paved_tile_state pts JOIN location_state loc ON pts.entity_id = loc.entity_id;" }},{m_global_2}
]
"""
m_global_2 = '\n    { name = "paving_tile_desc", output_folder = "global" },'

if mapper:
    outfile = "mapper.toml"
    output = m_h
    first = True
    m_offset_1 = 0
    m_offset_2 = 0
    for region in mapper:
        output += "\n[[upstream.regions]]\n"\
                  f"name = \"bitcraft-live-{region}\"\n"\
                  f"id = {region}\n"\
                  f"{m_d_1.format(m_offset_1=m_offset_1, m_global_1=m_global_1 if first else '')}"\
                  f"{m_d_2.format(m_offset_2=m_offset_2, m_global_2=m_global_2 if first else '')}"\
                  f""
        first = False
        m_offset_1 += 2
        m_offset_2 += 1
    output += m_p
    with open(os.path.join(out_dir, outfile), "w") as f:
        f.write(output)
