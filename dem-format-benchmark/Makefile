.PHONY: all data bench viz post deploy clean

all: data bench viz post

data:
	python scripts/data_prep.py

bench:
	python scripts/benchmark.py

viz:
	python scripts/visualize.py

post:
	python scripts/generate_post.py

deploy:
	cp content/*.mdx /Users/danielfisher/repositories/dnf0.github.io/content/blog/
	cp plots/*.png /Users/danielfisher/repositories/dnf0.github.io/public/plots/
	@echo "Deployed to dnf0.github.io."

clean:
	rm -rf data/* plots/*.png results/benchmarks.parquet results/tables/*
	@echo "Cleaned generated files. Source data preserved."
