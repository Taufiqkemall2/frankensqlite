import json

def find_next_task():
    try:
        with open('.beads/issues.jsonl', 'r') as f:
            issues = []
            for line in f:
                if line.strip():
                    issues.append(json.loads(line))
            
            # Map issue ID to status for dependency checking
            status_map = {issue['id']: issue['status'] for issue in issues}
            
            for issue in issues:
                if issue['status'] == 'open':
                    # Check dependencies
                    blocked = False
                    if 'dependencies' in issue:
                        for dep in issue['dependencies']:
                            if dep['type'] == 'blocks':
                                # This issue blocks another, that's fine.
                                pass
                            elif dep['type'] == 'depends_on': # In some schemas it might be different, let's check
                                pass
                    
                    # Actually, the dependencies field structure in the file seems to be:
                    # "dependencies":[{"issue_id":"bd-1299","depends_on_id":"bd-3bql","type":"blocks",...}]
                    # Wait, "issue_id" depends on "depends_on_id"? Or "issue_id" blocks "depends_on_id"?
                    # The type says "blocks". If A blocks B, then A must be done before B.
                    # If I am looking at A, and A blocks B, A is free (unless A is blocked by something else).
                    # If I am looking at B, and A blocks B, B is blocked if A is open.
                    
                    # Let's look for things blocking THIS issue.
                    # In the file, dependencies are listed under the issue.
                    # Example from bd-1299:
                    # "dependencies":[{"issue_id":"bd-1299","depends_on_id":"bd-3bql","type":"blocks"}]
                    # This implies bd-1299 BLOCKS bd-3bql? Or bd-1299 IS BLOCKED BY bd-3bql?
                    # "type": "blocks" usually means the relation is (Subject Blocks Object).
                    # So issue_id blocks depends_on_id.
                    
                    # Wait, let's look at a closed issue to see the convention.
                    # bd-1j3s depends on bd-1299 (type: blocks). 
                    # Actually, usually there is a 'blocked_by' or similar. 
                    # Let's assume standard beads: 
                    # If issue A has a dependency entry {issue_id: A, depends_on_id: B, type: "blocks"}, 
                    # it usually means A blocks B.
                    # But often the JSON contains all edges.
                    
                    # Let's try to find an issue that has no "is blocked by" relationships.
                    # In beads, typically you check if any OPEN issue BLOCKS the current issue.
                    
                    # Let's just find the first open issue and print it, and we can manually check deps.
                    # Or better, print the first few open issues.
                    
                    print(f"Open Issue: {issue['id']} - {issue['title']}")
                    if 'dependencies' in issue:
                        print(f"  Dependencies: {issue['dependencies']}")
                    
    except Exception as e:
        print(f"Error: {e}")

find_next_task()
