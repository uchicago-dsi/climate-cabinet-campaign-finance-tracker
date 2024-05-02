# Scope of Network Visualizations 

The way we aim to think about the network visualizations is through two views:
* a micro-level view (zoomed-in view of the network of transactions)
* a macro-level view (zoomed-out view of the clusters prevalent in network visualizations)

## Micro-level Visualizations 
This zoomed-in view focuses on detailed interactions between specific entities (individuals and organizations) within the network.

Questions we can aim to address through this visualization:
* Who are the major donors and recipients? 
    *  Identify key players in the network who contribute significant amounts or receive significant amounts of campaign finance.
* What are the characteristics of transactions between specific entities?
    * Analyze the number and direction of transactions, including the size of donations and the frequency of interactions between specific donors and recipients.
* How are entities related to each other?
    * Uncover relationships and influence patterns among entities through directions of transactions, showing direct connections and the flow of funds.
* How do contributions flow within different sectors (e.g., fossil fuel, clean energy)?
    * Explore how industries are supporting their interests through campaign contributions, distinguishing between different sectors like fossil fuel and clean energy.

## Macro-level Visualizations
This broader view provides insights into the overall structure and health of the network, revealing clusters and central nodes.

Questions we can aim to address through this visualization:
* What are the major clusters within the network?
    * Identify clusters within the network, which could represent alliances or collective action among groups with common interests.
* How centralized is the network?
    * Assess the centrality measures to understand how influence or control is distributed across the network. High centrality might indicate a few nodes with a lot of control or influence over the network.
* Are there identifiable patterns of influence among different types of entities?
    * Examine the roles of various entity types (e.g., individual donors, PACs, corporations) within the network and their influence patterns.
* How do the interactions between different classifications of entities (fossil fuel, clean energy, other) compare?
    * Compare the connectivity and interaction patterns between entities classified under different energy sectors, highlighting potential biases or alignments in political contributions.

## Other considerations 
* Further customize the appearance of nodes and edges based on additional metrics and data if it can be gathered (e.g., node size based on size of entity (donor comapnies for instance), edge color based on transaction type (more classification needed here)).
* Consider deploying community detection algorithms or temporal analysis to observe how connections and clusters evolve over time, to improve analytical depth.