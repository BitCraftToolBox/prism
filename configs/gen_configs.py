import os
import sys

region_groups = {
    # main regions
    3: [17, 18, 19],
    2: [12, 13, 14],
    1: [7, 8, 9],
    # temp regions
    4: [3, 11, 15, 23]
}

# extra dump config for global-mirrored state which we only want one copy of
global_dumps = 13

out_dir = sys.argv[1] if len(sys.argv) > 1 else ""

with open("prism.toml.template") as f:
    template = f.read()

with open("global_dumps.toml.template") as f:
    global_template = f.read()

before, after = template.split("#__REGIONS__#")

for group, regions in region_groups.items():
    outfile = f"prism-{group}.toml"
    output = "" + before
    for region in regions:
        output += "\n[[upstream.regions]]\n"\
                  f"name = \"bitcraft-live-{region}\"\n"\
                  f"id = {region}\n"
        if region == global_dumps:
            output += global_template
    output += after
    with open(os.path.join(out_dir, outfile), "w") as f:
        f.write(output)
