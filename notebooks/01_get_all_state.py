import json
import re
import os

def extract_states_from_html():
    """
    Extract state data from meta/state.html and create a JSON file
    """
    # Read the HTML file
    html_file_path = os.path.join('..', 'meta', 'state.html')
    
    with open(html_file_path, 'r', encoding='utf-8') as file:
        html_content = file.read()
    
    # Use regex to find all option elements with value and text
    option_pattern = r'<option[^>]*value="([^"]*)"[^>]*>([^<]*)</option>'
    matches = re.findall(option_pattern, html_content)
    
    # Extract states data (skip the first "-- Select --" option)
    states_data = []
    
    for value, text in matches:
        text = text.strip()
        
        # Skip the placeholder option
        if value and value != 'none':
            states_data.append({
                'value': value,
                'text': text
            })
    
    # Sort by value for better organization
    states_data.sort(key=lambda x: x['value'])
    
    # Create the final JSON structure
    result = {
        'states': states_data,
        'total_count': len(states_data)
    }
    
    # Save to JSON file
    output_file = os.path.join('..', 'meta_results', 'states_data.json')
    with open(output_file, 'w', encoding='utf-8') as json_file:
        json.dump(result, json_file, indent=2, ensure_ascii=False)
    
    print(f"Successfully extracted {len(states_data)} states to {output_file}")
    
    # Display first few entries as preview
    print("\nFirst 5 states:")
    for state in states_data[:5]:
        print(f"  {state['value']}: {state['text']}")
    
    return result

if __name__ == "__main__":
    # Extract states data
    states_json = extract_states_from_html()
    
    # Print summary
    print(f"\nTotal states/territories extracted: {states_json['total_count']}")