"""
Example 17: Real-World Advanced Example - Data Processing Pipeline

A complete data processing pipeline using generators:
- Read JSON lines from a file
- Filter events by type
- Transform events
- Batch items for processing
"""

import json
import os
import tempfile
from typing import Iterator, Dict, List


def read_json_lines(filename: str) -> Iterator[Dict]:
    """Read newline-delimited JSON file."""
    with open(filename) as f:
        for line in f:
            yield json.loads(line)


def filter_events(events: Iterator[Dict], event_type: str) -> Iterator[Dict]:
    """Filter events by type."""
    for event in events:
        if event.get('type') == event_type:
            yield event


def transform_event(events: Iterator[Dict]) -> Iterator[Dict]:
    """Transform events."""
    for event in events:
        yield {
            'timestamp': event['ts'],
            'user': event['user_id'],
            'action': event['action']
        }


def batch(iterator: Iterator, n: int) -> Iterator[list]:
    """Batch items into groups of n."""
    batch_items = []
    for item in iterator:
        batch_items.append(item)
        if len(batch_items) == n:
            yield batch_items
            batch_items = []
    if batch_items:
        yield batch_items


def process_batch(batch_of_events):
    """Process a batch of events."""
    print(f"  Processing batch of {len(batch_of_events)} events")


if __name__ == "__main__":
    # Create a temporary JSONL file with sample data
    with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.jsonl') as f:
        test_file = f.name
        # Write sample events
        events = [
            {'type': 'click', 'ts': '2024-01-01T10:00:00', 'user_id': 'user1', 'action': 'button_click'},
            {'type': 'view', 'ts': '2024-01-01T10:01:00', 'user_id': 'user2', 'action': 'page_view'},
            {'type': 'click', 'ts': '2024-01-01T10:02:00', 'user_id': 'user1', 'action': 'link_click'},
            {'type': 'click', 'ts': '2024-01-01T10:03:00', 'user_id': 'user3', 'action': 'button_click'},
            {'type': 'view', 'ts': '2024-01-01T10:04:00', 'user_id': 'user2', 'action': 'page_view'},
            {'type': 'click', 'ts': '2024-01-01T10:05:00', 'user_id': 'user1', 'action': 'button_click'},
        ]
        for event in events:
            f.write(json.dumps(event) + '\n')
    
    try:
        print("Building generator pipeline:")
        print("read_json_lines -> filter_events -> transform_event -> batch\n")
        
        # Use the pipeline
        pipeline = batch(
            transform_event(
                filter_events(
                    read_json_lines(test_file),
                    'click'
                )
            ),
            2  # Batch size of 2
        )
        
        print("Processing batches:")
        for batch_of_events in pipeline:
            process_batch(batch_of_events)
            print(f"    Batch contents: {batch_of_events}")
        
        print("\n✅ Pipeline processes data efficiently without loading all into memory!")
    finally:
        # Clean up
        os.unlink(test_file)

