TABLES := content skipped_content directory directory_entry_file \
	directory_entry_dir directory_entry_rev person revision revision_history \
	release snapshot snapshot_branches snapshot_branch origin origin_visit

DATASETS := $(addprefix datasets/swh_graph_,$(TABLES))
OVERVIEWS := $(addsuffix /overview.md,$(DATASETS))
CONFIGS := $(addsuffix /config.json,$(DATASETS))
TARGETS := $(OVERVIEWS) $(CONFIGS) datasets/swh_graph/config.json

all: $(TARGETS)

datasets/swh_graph_%/overview.md:
	mkdir -p $$( dirname $@ )
	cat dataset_stub.md > $@
	sed -n '/^- \+\*\*$*\*\*/,/^-/p' ../schema.rst | head -n-1 >> $@

datasets/swh_graph/config.json: config_template.json
	cat config_template.json |\
		jq '.Id = "software-heritage-graph-dataset"' |\
		jq '.Slug = "software-heritage-graph-dataset"' |\
		jq '.Name = "Software Heritage Graph Dataset"' |\
		jq '.DataAccess.AzureDatabricks.python."azureml-opendatasets" = "Notebooks/software-heritage-graph-dataset/swh-graph-example-notebook.ipynb"' \
		> $@

datasets/swh_graph_%/config.json:
	cat config_template.json |\
		jq '.Id = "software-heritage-graph-dataset-$*"' |\
		jq '.Slug = "software-heritage-graph-dataset-$*"' |\
		jq '.Name = "Software Heritage Graph Dataset: $* table"' |\
		jq '.BlobLocation.Path = "swhgraph/2018-09-25/parquet/$*"' \
		> $@
