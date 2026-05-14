import numpy as np
import pandas as pd

# Simulate a polygon that covers 1000 pixels of a lake (elevation 0)
# and 100 pixels of a mountain cliff (elevation 2500)

lake_pixels = 1000
mountain_pixels = 100

true_avg = (lake_pixels * 0 + mountain_pixels * 2500) / (lake_pixels + mountain_pixels)

# Under quadtree compaction, the 1000 lake pixels might compress into just a few parent cells.
# Let's say it compresses perfectly into 1 giant cell (Level 5+).
compacted_lake_cells = 1

# The mountain is steep and rugged, so it doesn't compact well. 
# Let's say the 100 pixels compact into 80 cells.
compacted_mountain_cells = 80

compacted_avg = (compacted_lake_cells * 0 + compacted_mountain_cells * 2500) / (compacted_lake_cells + compacted_mountain_cells)

print(f"True Area-Weighted Average: {true_avg:.1f} meters")
print(f"Compacted (Unweighted) Average: {compacted_avg:.1f} meters")
print(f"Error: {abs(compacted_avg - true_avg):.1f} meters")
