#!/usr/bin/env python3
"""
Convert TAAFT schema to MCP schema for compatibility with agent-indexing and probing-pipeline.

TAAFT -> MCP mapping:
- slug -> agent_id
- name -> name
- external_url -> source_url
- description -> description
- pricing_model -> pricing
- task_categories -> detected_capabilities
- rating/rating_count -> community_rating/rating_count
- pros -> llm_extracted.capabilities
- cons -> llm_extracted.limitations
"""

import json
import uuid
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List


def slugify_id(slug: str) -> str:
    """Generate a hash-like agent_id from slug."""
    # Use part of UUID hash as agent_id to match MCP format
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, slug)).replace('-', '')[:16]


def normalize_pricing(pricing_model: str) -> str:
    """Convert TAAFT pricing_model to MCP pricing format."""
    if "free" in pricing_model.lower():
        return "free"
    elif "freemium" in pricing_model.lower():
        return "freemium"
    elif "open_source" in pricing_model.lower():
        return "open_source"
    else:
        return "unknown"


def extract_capabilities_from_description(description: str, task_categories: List[str]) -> List[str]:
    """
    Extract capabilities from description and task_categories.
    Returns the concatenation of task categories and first few capability keywords.
    """
    # Start with task categories
    capabilities = list(task_categories) if task_categories else []
    
    # Extract action verbs/capabilities from description (first sentence)
    first_sentence = description.split('.')[0] if description else ""
    keywords = []
    
    action_words = ['generate', 'create', 'analyze', 'process', 'manage', 'convert', 'summarize', 
                   'chat', 'search', 'retrieve', 'organize', 'collaborate', 'automate', 'transcribe',
                   'format', 'enhance', 'extract', 'transform', 'optimize']
    
    for word in action_words:
        if word.lower() in first_sentence.lower():
            keywords.append(word.capitalize())
    
    capabilities.extend(keywords[:3])  # Add up to 3 capability keywords
    return list(dict.fromkeys(capabilities))  # Remove duplicates, preserve order


def convert_taaft_to_mcp(taaft_agent: Dict[str, Any]) -> Dict[str, Any]:
    """Convert a single TAAFT agent to MCP schema."""
    
    # Extract TAAFT fields
    slug = taaft_agent.get('slug', 'unknown')
    name = taaft_agent.get('name', '')
    description = taaft_agent.get('description', '')
    pricing_model = taaft_agent.get('pricing_model', 'Unknown')
    task_categories = taaft_agent.get('task_categories', [])
    pros = taaft_agent.get('pros', [])
    cons = taaft_agent.get('cons', [])
    rating = taaft_agent.get('rating')
    rating_count = taaft_agent.get('rating_count', 0)
    external_url = taaft_agent.get('external_url', '')
    traffic_count = taaft_agent.get('traffic_count')
    last_updated = taaft_agent.get('last_updated', '')
    
    # Build MCP schema
    mcp_agent = {
        'agent_id': slugify_id(slug),
        'name': name,
        'source': 'taaft',
        'source_url': external_url,
        'description': description,
        
        # Tools: placeholder since TAAFT doesn't have detailed tool specs
        'tools': [
            {
                'name': f'{name.replace(" ", "_").lower()}_main_action',
                'description': f'Main functionality of {name}'
            }
        ] if name else [],
        
        # Capabilities from task_categories and description
        'detected_capabilities': extract_capabilities_from_description(description, task_categories),
        'llm_backbone': 'Unknown',
        
        # Rating fields
        'arena_elo': None,
        'arena_battles': None,
        'community_rating': rating,
        'rating_count': rating_count,
        
        # Pricing and timestamps
        'pricing': normalize_pricing(pricing_model),
        'last_updated': last_updated or datetime.now().isoformat(),
        'indexed_at': datetime.now().isoformat(),
        
        # Embeddings
        'description_embedding': None,
        'testability_tier': 'n/a',
        
        # Documentation (use TAAFT data as fallback)
        'documentation': {
            'readme': description,
            'detail_page': external_url
        },
        
        'documentation_chunks': [],
        'documentation_quality': 0.5 if description else 0.1,  # TAAFT provides descriptions
        'quality_rationale': 'Converted from TAAFT public tool listing',
        'llm_text_source': 'description_only',
        
        # Extracted structured data
        'llm_extracted': {
            'capabilities': pros[:5] if pros else extract_capabilities_from_description(description, task_categories),
            'limitations': cons[:5] if cons else [],
            'requirements': ['Internet connection'] if 'offline' not in description.lower() else ['None']
        },
        
        # TAAFT-specific metadata (preserved for reference)
        '_taaft_metadata': {
            'slug': slug,
            'taaft_url': taaft_agent.get('taaft_url', ''),
            'traffic_count': traffic_count,
            'leaderboard_score': taaft_agent.get('leaderboard_score'),
            'is_agent': taaft_agent.get('is_agent', True),
            'scraped_at': taaft_agent.get('scraped_at', '')
        }
    }
    
    return mcp_agent


def convert_taaft_file(input_file: str, output_file: str) -> None:
    """Convert entire TAAFT agents.json file to MCP schema."""
    
    input_path = Path(input_file)
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    # Load TAAFT data
    with open(input_path, 'r', encoding='utf-8') as f:
        taaft_agents = json.load(f)
    
    # Convert all agents
    mcp_agents = []
    for taaft_agent in taaft_agents:
        try:
            mcp_agent = convert_taaft_to_mcp(taaft_agent)
            mcp_agents.append(mcp_agent)
        except Exception as e:
            print(f"Warning: Failed to convert {taaft_agent.get('slug', 'unknown')}: {e}")
            continue
    
    # Write MCP format
    output_path = Path(output_file)
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(mcp_agents, f, indent=2, ensure_ascii=False)
    
    print(f"✓ Converted {len(mcp_agents)} agents")
    print(f"✓ Output: {output_path.absolute()}")


def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Convert TAAFT agents.json to MCP schema format'
    )
    parser.add_argument(
        '-i', '--input',
        default='agents.json',
        help='Input TAAFT agents.json file (default: agents.json)'
    )
    parser.add_argument(
        '-o', '--output',
        default='agents_mcp.json',
        help='Output MCP format file (default: agents_mcp.json)'
    )
    parser.add_argument(
        '--keep-taaft',
        action='store_true',
        help='Include TAAFT metadata in output (_taaft_metadata field)'
    )
    
    args = parser.parse_args()
    
    try:
        convert_taaft_file(args.input, args.output)
        print(f"\nFile ready to use with:")
        print(f"  - agent-indexing (build_index.py)")
        print(f"  - probing-pipeline (run_real_scenarios.py)")
    except Exception as e:
        print(f"Error: {e}")
        exit(1)


if __name__ == '__main__':
    main()
