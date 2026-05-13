import json

with open("results.json") as f:
    data = json.load(f)

benchmarks = data["benchmarks"]
benchmarks.sort(key=lambda x: x["stats"]["mean"])

print("```text")
print("-" * 123)
print(f"{'Name (time in us)':<40} {'Min':<23} {'Max':<21} {'Mean':<19} {'StdDev':<17} {'Rounds'}")
print("-" * 123)

for b in benchmarks:
    name = b["name"]
    stats = b["stats"]
    min_val = stats["min"] * 1e6
    max_val = stats["max"] * 1e6
    mean_val = stats["mean"] * 1e6
    stddev = stats["stddev"] * 1e6
    rounds = stats["rounds"]
    
    # We'll just print simple values, ignoring the IQR/ops stuff for brevity since it's just a blog post
    print(f"{name:<40} {min_val:>10.4f}             {max_val:>10.4f}           {mean_val:>10.4f}        {stddev:>10.4f}           {rounds}")
print("-" * 123)
print("```")
