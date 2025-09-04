# connectors/undp.py
def fetch():
    # Return a dummy list to test the pipeline
    return [
        {
            "title": "Test UNDP Governance Grant",
            "url": "https://example.org/fake-undp-call",
            "deadline": "2025-12-31",
            "summary": "Dummy funding call for testing",
            "region": "Africa",
            "themes": "anti_corruption"
        }
    ]
