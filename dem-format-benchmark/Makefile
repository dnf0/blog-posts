.PHONY: all data bench viz post deploy clean sync preview export

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

preview:
	@echo "Setting up dynamic symlinks for all blog posts (live reload enabled)..."
	@rm -rf blog-preview/public/plots blog-preview/app/posts
	@mkdir -p blog-preview/app/posts blog-preview/public
	@ln -sF $(PWD)/plots blog-preview/public/plots
	@find $(PWD)/content -name "*.mdx" | while read file; do \
		rel_path=$${file#$(PWD)/content/}; \
		base=$${rel_path%.mdx}; \
		mkdir -p "blog-preview/app/posts/$$base"; \
		ln -sf "$$file" "blog-preview/app/posts/$$base/page.mdx"; \
		echo "  -> Mapped: http://localhost:3000/posts/$$base"; \
	done
	cd blog-preview && npm run dev

export:
	@echo "Preparing all blog posts for static export..."
	@rm -rf blog-preview/public/plots blog-preview/app/posts
	@mkdir -p blog-preview/app/posts blog-preview/public
	@cp -R plots/ blog-preview/public/plots/
	@find content -name "*.mdx" | while read file; do \
		rel_path=$${file#content/}; \
		base=$${rel_path%.mdx}; \
		mkdir -p "blog-preview/app/posts/$$base"; \
		cp "$$file" "blog-preview/app/posts/$$base/page.mdx"; \
	done
	cd blog-preview && npm run build

deploy:
	cp content/*.mdx /Users/danielfisher/repositories/dnf0.github.io/content/blog/
	cp plots/*.png /Users/danielfisher/repositories/dnf0.github.io/public/plots/
	@echo "Deployed to dnf0.github.io."

clean:
	rm -rf data/* plots/*.png results/benchmarks.parquet results/tables/*
	@echo "Cleaned generated files. Source data preserved."
