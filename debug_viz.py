
try:
    with open("visualization_of_the_evolution_of_the_frankensqlite_specs_document_from_inception.html", "r", encoding="utf-8") as f:
        content = f.read()
    print(f"Read {len(content)} chars")
except Exception as e:
    print(f"Error: {e}")
