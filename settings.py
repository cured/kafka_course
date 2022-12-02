opensearch_host = "localhost"
opensearch_port = "9200"
opensearch_user = "admin"
opensearch_pass = "admin"
opensearch_index_name = "wikimedia"

batch_size = 1000
try:
    from settings_local import *
except Exception:
    pass
