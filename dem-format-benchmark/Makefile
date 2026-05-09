.PHONY: all data bench viz post deploy clean sync

all: data bench viz post

sync:
	uv sync

data:
	uv run python scripts/data_prep.py

bench:
	uv run python scripts/benchmark.py

viz:
	uv run python scripts/visualize.py

post:
	uv run python scripts/generate_post.py

deploy:
	cp content/*.mdx /Users/danielfisher/repositories/dnf0.github.io/content/blog/
	cp plots/*.png /Users/danielfisher/repositories/dnf0.github.io/public/plots/
	@echo "Deployed to dnf0.github.io."

clean:
	rm -rf data/* plots/*.png results/benchmarks.parquet results/tables/*
	@echo "Cleaned generated files. Source data preserved."
