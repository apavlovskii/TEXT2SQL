import pandas as pd
import numpy as np
from scipy.spatial.distance import cosine

# Load the data
data = pd.read_csv('/workspace/patents_filtered.csv')

# Convert citation column from string to list
data['citation'] = data['citation'].apply(eval)

# Filter patents filed within a month of the filing date
data['filing_date'] = pd.to_datetime(data['filing_date'], format='%Y%m%d')
data['citation_date'] = data['citation'].apply(lambda x: [pd.to_datetime(c['filing_date'], format='%Y%m%d') for c in x if c['filing_date'] != 0])
data['forward_citations_within_month'] = data.apply(lambda row: [c for c in row['citation_date'] if (c - row['filing_date']).days <= 30], axis=1)

# Find the patent with the most forward citations within a month
most_cited_patent = data.loc[data['forward_citations_within_month'].apply(len).idxmax()]

# Extract the filing year of the most cited patent
filing_year = most_cited_patent['filing_date'].year

# Filter patents from the same filing year
same_year_patents = data[data['filing_date'].dt.year == filing_year]

# Calculate similarity using embedding vectors
most_cited_embedding = np.array(eval(most_cited_patent['embedding_v1']))
same_year_patents['similarity'] = same_year_patents['embedding_v1'].apply(lambda x: 1 - cosine(most_cited_embedding, np.array(eval(x))))

# Find the most similar patent
most_similar_patent = same_year_patents.loc[same_year_patents['similarity'].idxmax()]

# Save the results
result = pd.DataFrame({
    'Most Cited Patent': [most_cited_patent['publication_number']],
    'Most Similar Patent': [most_similar_patent['publication_number']]
})
result.to_csv('/workspace/result.csv', index=False)
