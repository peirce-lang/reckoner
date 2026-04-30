from snf_peirce.compile import Substrate
from snf_peirce import query as pq
import duckdb

conn = duckdb.connect('substrates/discogsv1.duckdb', read_only=True)
s = Substrate(conn, 'discogs_v1')
r = pq(s, 'WHO.artist = "Elvis Presley" AND WHO.label ONLY "Spirit Of America Records"', limit=None)
print(r.count)
print(r.entity_ids[:5])