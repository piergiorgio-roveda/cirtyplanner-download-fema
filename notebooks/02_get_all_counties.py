import json
import requests
import time
import os
from typing import Dict, List, Any

def load_states_data() -> Dict[str, Any]:
    """
    Load states data from the JSON file
    """
    states_file = os.path.join('..', 'meta_results', 'states_data.json')
    with open(states_file, 'r', encoding='utf-8') as file:
        return json.load(file)

def fetch_counties_for_state(state_value: str, state_name: str) -> List[Dict[str, str]]:
    """
    Fetch counties for a specific state from FEMA API
    """
    url = f"https://msc.fema.gov/portal/advanceSearch?getCounty={state_value}"
    
    try:
        print(f"Fetching counties for {state_name} (value: {state_value})...")
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Parse JSON response
        counties_data = response.json()
        
        print(f"  Found {len(counties_data)} counties for {state_name}")
        return counties_data
        
    except requests.exceptions.RequestException as e:
        print(f"  Error fetching data for {state_name}: {e}")
        return []
    except json.JSONDecodeError as e:
        print(f"  Error parsing JSON for {state_name}: {e}")
        return []

def fetch_all_counties():
    """
    Fetch counties for all states and create comprehensive JSON
    """
    # Load states data
    states_data = load_states_data()
    
    # Initialize result structure
    all_counties = {
        "metadata": {
            "total_states": len(states_data["states"]),
            "total_counties": 0,
            "fetch_timestamp": time.strftime("%Y-%m-%d %H:%M:%S UTC", time.gmtime())
        },
        "states": {}
    }
    
    total_counties = 0
    
    # Process each state
    for state in states_data["states"]:
        state_value = state["value"]
        state_name = state["text"]
        
        # Fetch counties for this state
        counties = fetch_counties_for_state(state_value, state_name)
        
        # Store in result structure
        all_counties["states"][state_value] = {
            "state_name": state_name,
            "state_code": state_value,
            "county_count": len(counties),
            "counties": counties
        }
        
        total_counties += len(counties)
        
        # Add small delay to be respectful to the API
        time.sleep(0.5)
    
    # Update total count
    all_counties["metadata"]["total_counties"] = total_counties
    
    # Save to JSON file
    output_file = os.path.join('..', 'meta_results', 'all_counties_data.json')
    with open(output_file, 'w', encoding='utf-8') as json_file:
        json.dump(all_counties, json_file, indent=2, ensure_ascii=False)
    
    print(f"\nSuccessfully fetched county data for all states!")
    print(f"Total counties: {total_counties}")
    print(f"Data saved to: {output_file}")
    
    # Create summary report
    create_summary_report(all_counties)
    
    return all_counties

def create_summary_report(all_counties: Dict[str, Any]):
    """
    Create a summary report of the county data
    """
    summary = {
        "summary": {
            "total_states": all_counties["metadata"]["total_states"],
            "total_counties": all_counties["metadata"]["total_counties"],
            "fetch_timestamp": all_counties["metadata"]["fetch_timestamp"]
        },
        "states_summary": []
    }
    
    # Create summary for each state
    for state_code, state_data in all_counties["states"].items():
        summary["states_summary"].append({
            "state_code": state_code,
            "state_name": state_data["state_name"],
            "county_count": state_data["county_count"]
        })
    
    # Sort by county count (descending)
    summary["states_summary"].sort(key=lambda x: x["county_count"], reverse=True)
    
    # Save summary
    summary_file = os.path.join('..', 'meta_results', 'counties_summary.json')
    with open(summary_file, 'w', encoding='utf-8') as file:
        json.dump(summary, file, indent=2, ensure_ascii=False)
    
    print(f"Summary report saved to: {summary_file}")
    
    # Print top 10 states by county count
    print("\nTop 10 states by county count:")
    for i, state in enumerate(summary["states_summary"][:10], 1):
        print(f"  {i:2d}. {state['state_name']}: {state['county_count']} counties")

if __name__ == "__main__":
    # Check if requests library is available
    try:
        import requests
    except ImportError:
        print("Error: requests library is required. Install it with: pip install requests")
        exit(1)
    
    # Fetch all counties data
    try:
        all_counties_data = fetch_all_counties()
        print("\nTask completed successfully!")
    except Exception as e:
        print(f"Error during execution: {e}")
        exit(1)