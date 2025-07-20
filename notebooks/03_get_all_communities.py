import json
import requests
import time
import os
from typing import Dict, List, Any

def load_counties_data() -> Dict[str, Any]:
    """
    Load counties data from the JSON file
    """
    counties_file = os.path.join('..', 'meta_results', 'all_counties_data.json')
    with open(counties_file, 'r', encoding='utf-8') as file:
        return json.load(file)

def fetch_communities_for_county(county_value: str, state_code: str, county_name: str, state_name: str) -> List[Dict[str, str]]:
    """
    Fetch communities for a specific county from FEMA API
    """
    url = f"https://msc.fema.gov/portal/advanceSearch?getCommunity={county_value}&state={state_code}"
    
    try:
        print(f"Fetching communities for {county_name}, {state_name} (county: {county_value}, state: {state_code})...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse JSON response
        communities_data = response.json()
        
        print(f"  Found {len(communities_data)} communities for {county_name}")
        return communities_data
        
    except requests.exceptions.RequestException as e:
        print(f"  Error fetching data for {county_name}: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"  Error parsing JSON for {county_name}: {e}")
        return []

def fetch_all_communities():
    """
    Fetch communities for all counties and create comprehensive JSON
    """
    # Load counties data
    counties_data = load_counties_data()
    
    # Initialize result structure
    all_communities = {
        "metadata": {
            "total_states": counties_data["metadata"]["total_states"],
            "total_counties": counties_data["metadata"]["total_counties"],
            "total_communities": 0,
            "fetch_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        },
        "states": {}
    }
    
    total_communities = 0
    processed_counties = 0
    
    # Process each state
    for state_code, state_data in counties_data["states"].items():
        state_name = state_data["state_name"]
        counties = state_data["counties"]
        
        print(f"\n=== Processing {state_name} ({len(counties)} counties) ===")
        
        # Initialize state structure
        all_communities["states"][state_code] = {
            "state_name": state_name,
            "state_code": state_code,
            "county_count": len(counties),
            "community_count": 0,
            "counties": {}
        }
        
        state_community_count = 0
        
        # Process each county in the state
        for county in counties:
            county_value = county["value"]
            county_name = county["label"]
            
            # Fetch communities for this county
            communities = fetch_communities_for_county(county_value, state_code, county_name, state_name)
            
            # Store in result structure
            all_communities["states"][state_code]["counties"][county_value] = {
                "county_name": county_name,
                "county_code": county_value,
                "community_count": len(communities),
                "communities": communities
            }
            
            state_community_count += len(communities)
            total_communities += len(communities)
            processed_counties += 1
            
            # Add small delay to be respectful to the API
            time.sleep(0.3)
            
            # Progress update every 10 counties
            if processed_counties % 10 == 0:
                print(f"  Progress: {processed_counties}/{counties_data['metadata']['total_counties']} counties processed")
        
        # Update state community count
        all_communities["states"][state_code]["community_count"] = state_community_count
        print(f"  {state_name} completed: {state_community_count} communities in {len(counties)} counties")
    
    # Update total count
    all_communities["metadata"]["total_communities"] = total_communities
    
    # Save to JSON file
    output_file = os.path.join('..', 'meta_results', 'all_communities_data.json')
    with open(output_file, 'w', encoding='utf-8') as json_file:
        json.dump(all_communities, json_file, indent=2, ensure_ascii=False)
    
    print(f"\n=== FINAL RESULTS ===")
    print(f"Successfully fetched community data for all counties!")
    print(f"Total states: {counties_data['metadata']['total_states']}")
    print(f"Total counties: {counties_data['metadata']['total_counties']}")
    print(f"Total communities: {total_communities}")
    print(f"Data saved to: {output_file}")
    
    # Create summary report
    create_summary_report(all_communities)
    
    return all_communities

def create_summary_report(all_communities: Dict[str, Any]):
    """
    Create a summary report of the community data
    """
    summary = {
        "summary": {
            "total_states": all_communities["metadata"]["total_states"],
            "total_counties": all_communities["metadata"]["total_counties"],
            "total_communities": all_communities["metadata"]["total_communities"],
            "fetch_timestamp": all_communities["metadata"]["fetch_timestamp"]
        },
        "states_summary": [],
        "counties_with_most_communities": []
    }
    
    # Create summary for each state and collect county data
    all_counties_list = []
    
    for state_code, state_data in all_communities["states"].items():
        summary["states_summary"].append({
            "state_code": state_code,
            "state_name": state_data["state_name"],
            "county_count": state_data["county_count"],
            "community_count": state_data["community_count"]
        })
        
        # Collect county data for top counties report
        for county_code, county_data in state_data["counties"].items():
            all_counties_list.append({
                "state_name": state_data["state_name"],
                "state_code": state_code,
                "county_name": county_data["county_name"],
                "county_code": county_code,
                "community_count": county_data["community_count"]
            })
    
    # Sort states by community count (descending)
    summary["states_summary"].sort(key=lambda x: x["community_count"], reverse=True)
    
    # Sort counties by community count and get top 20
    all_counties_list.sort(key=lambda x: x["community_count"], reverse=True)
    summary["counties_with_most_communities"] = all_counties_list[:20]
    
    # Save summary
    summary_file = os.path.join('..', 'meta_results', 'communities_summary.json')
    with open(summary_file, 'w', encoding='utf-8') as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)
    
    print(f"Summary report saved to: {summary_file}")
    
    # Print top 10 states by community count
    print("\nTop 10 states by community count:")
    for i, state in enumerate(summary["states_summary"][:10], 1):
        print(f"  {i:2d}. {state['state_name']}: {state['community_count']} communities ({state['county_count']} counties)")
    
    # Print top 10 counties by community count
    print("\nTop 10 counties by community count:")
    for i, county in enumerate(summary["counties_with_most_communities"][:10], 1):
        print(f"  {i:2d}. {county['county_name']}, {county['state_name']}: {county['community_count']} communities")

if __name__ == "__main__":
    # Check if requests library is available
    try:
        import requests
    except ImportError:
        print("Error: requests library is required. Install it with: pip install requests")
        exit(1)
    
    # Fetch all communities data
    try:
        print("Starting community data collection...")
        print("This may take a while as we need to fetch data for 3,176+ counties...")
        print("=" * 60)
        
        all_communities_data = fetch_all_communities()
        print("\nTask completed successfully!")
        
    except KeyboardInterrupt:
        print("\n\nProcess interrupted by user. Partial data may have been saved.")
        exit(1)
    except Exception as e:
        print(f"Error during execution: {e}")
        exit(1)