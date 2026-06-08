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

with open("prism-map-dumps.toml.template") as f:
    map_dump_template = f.read()

with open("compose.yml.template") as f:
    compose_template = f.read()

compose_header, compose_service = compose_template.split("$SERVICE$", 1)

before, after = prism_template.split("$REGIONS$")

map_before, map_after = map_dump_template.split("$REGIONS$")

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

if mapper:
    outfile = "mapper.toml"
    output = map_before
    for region in mapper:
        output += "\n[[upstream.regions]]\n"\
                  f"name = \"bitcraft-live-{region}\"\n"\
                  f"id = {region}\n"
    output += map_after
    with open(os.path.join(out_dir, outfile), "w") as f:
        f.write(output)

with open(os.path.join(out_dir, "docker-compose.prism.yml"), "w") as f:
    f.write(compose_output)
