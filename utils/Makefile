all: all_individuals.csv, all_organizations.csv, all_transactions.csv

#set this up as a pipeline, check how far along Adil is on this
output deduplicated_individuals: all_individuals.csv
	python deduplication_pipeline

#likewise here
output deduplicated_organizations: all_organizations.csv
	python deduplication_pipeline

output classified_individuals: all_individuals.csv
	python classifier_pipeline

output classified_organizations: all_organizations.csv
	python classifier_pipeline

output network_graph: all_individuals.csv all_organizations.csv all_transactions.csv
	python network_graph_pipeline