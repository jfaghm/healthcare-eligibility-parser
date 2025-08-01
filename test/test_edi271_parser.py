#!/usr/bin/env python3
"""
Simple EDI 271 Parser - No Database Required
Just parses EDI files and generates reports
"""

import argparse
import json
import os
from dataclasses import dataclass, asdict

@dataclass
class EligibilityResponse:
    transaction_id: str = ""
    response_date: str = ""
    payer_name: str = ""
    provider_name: str = ""
    provider_npi: str = ""
    subscriber_name: str = ""
    member_id: str = ""
    group_number: str = ""
    employer: str = ""
    address: str = ""
    date_of_birth: str = ""
    gender: str = ""
    plan_name: str = ""
    individual_deductible: str = ""
    individual_deductible_met: str = ""
    status: str = "Active"

class SimpleEDI271Parser:
    def __init__(self):
        self.data = EligibilityResponse()
    
    def parse_file(self, file_path: str) -> EligibilityResponse:
        with open(file_path, 'r') as f:
            content = f.read().strip()
        return self.parse_content(content)
    
    def parse_content(self, content: str) -> EligibilityResponse:
        segments = [seg.strip() for seg in content.split('~') if seg.strip()]
        
        for segment in segments:
            if not segment:
                continue
            elements = segment.split('*')
            segment_type = elements[0]
            
            if segment_type == 'ST' and len(elements) > 2:
                self.data.transaction_id = elements[2]
            
            elif segment_type == 'BHT' and len(elements) > 4:
                date_str = elements[4]
                if len(date_str) == 8:
                    self.data.response_date = f"{date_str[4:6]}/{date_str[6:8]}/{date_str[:4]}"
            
            elif segment_type == 'NM1':
                if len(elements) > 1:
                    entity_type = elements[1]
                    if entity_type == 'PR' and len(elements) > 3:  # Payer
                        self.data.payer_name = elements[3]
                    elif entity_type == '1P' and len(elements) > 3:  # Provider
                        self.data.provider_name = elements[3]
                        if len(elements) > 9:
                            self.data.provider_npi = elements[9]
                    elif entity_type == 'IL':  # Subscriber
                        if len(elements) > 4:
                            last = elements[3] if len(elements) > 3 else ""
                            first = elements[4] if len(elements) > 4 else ""
                            middle = elements[5] if len(elements) > 5 else ""
                            self.data.subscriber_name = f"{last}, {first}"
                            if middle:
                                self.data.subscriber_name += f" {middle}"
                        if len(elements) > 9:
                            self.data.member_id = elements[9]
            
            elif segment_type == 'REF' and len(elements) > 2:
                ref_type = elements[1]
                if ref_type == '18':
                    self.data.group_number = elements[2]
                elif ref_type == '6P' and len(elements) > 3:
                    self.data.employer = elements[3]
            
            elif segment_type == 'N3' and len(elements) > 1:
                self.data.address = elements[1]
            
            elif segment_type == 'N4' and len(elements) > 3 and self.data.address:
                city = elements[1]
                state = elements[2]
                zip_code = elements[3]
                self.data.address += f", {city}, {state} {zip_code}"
            
            elif segment_type == 'DMG':
                if len(elements) > 2:
                    date_str = elements[2]
                    if len(date_str) == 8:
                        self.data.date_of_birth = f"{date_str[4:6]}/{date_str[6:8]}/{date_str[:4]}"
                if len(elements) > 3:
                    self.data.gender = "Female" if elements[3] == 'F' else "Male"
            
            elif segment_type == 'EB':
                if len(elements) > 4 and 'PLAN' in elements[4].upper():
                    self.data.plan_name = elements[4]
                
                # Parse financial amounts
                if len(elements) > 7:
                    amount = elements[7]
                    if amount and amount.replace('.', '').replace('-', '').isdigit():
                        formatted_amount = f"${float(amount):,.2f}"
                        
                        if len(elements) > 1:
                            benefit_type = elements[1]
                            if benefit_type == 'C':  # Coverage
                                coverage_level = elements[2] if len(elements) > 2 else ""
                                time_period = elements[6] if len(elements) > 6 else ""
                                
                                if time_period == '22' and coverage_level == 'IND':
                                    if not self.data.individual_deductible:
                                        self.data.individual_deductible = formatted_amount
                                elif time_period == '29' and coverage_level == 'IND':
                                    if not self.data.individual_deductible_met:
                                        self.data.individual_deductible_met = formatted_amount
        
        return self.data

def generate_html_report(data: EligibilityResponse, output_file: str):
    html_content = f"""
<!DOCTYPE html>
<html>
<head>
    <title>EDI 271 Report</title>
    <style>
        body {{ font-family: Arial, sans-serif; margin: 20px; }}
        .container {{ max-width: 800px; }}
        ul {{ line-height: 1.6; }}
        .header {{ color: #333; border-bottom: 2px solid #007acc; padding-bottom: 10px; }}
    </style>
</head>
<body>
    <div class="container">
        <h1 class="header">EDI 271 Eligibility Response Report</h1>
        <ul>
            <li><strong>Transaction ID:</strong> {data.transaction_id}</li>
            <li><strong>Response Date:</strong> {data.response_date}</li>
            <li><strong>Payer:</strong> {data.payer_name}</li>
            <li><strong>Provider:</strong> {data.provider_name}</li>
            <li><strong>Provider NPI:</strong> {data.provider_npi}</li>
            <li><strong>Subscriber:</strong> {data.subscriber_name}</li>
            <li><strong>Member ID:</strong> {data.member_id}</li>
            <li><strong>Group Number:</strong> {data.group_number}</li>
            <li><strong>Employer:</strong> {data.employer}</li>
            <li><strong>Address:</strong> {data.address}</li>
            <li><strong>Date of Birth:</strong> {data.date_of_birth}</li>
            <li><strong>Gender:</strong> {data.gender}</li>
            <li><strong>Plan:</strong> {data.plan_name}</li>
            <li><strong>Individual Deductible:</strong> {data.individual_deductible}</li>
            <li><strong>Individual Deductible Met:</strong> {data.individual_deductible_met}</li>
            <li><strong>Status:</strong> {data.status}</li>
        </ul>
    </div>
</body>
</html>
"""
    
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    with open(output_file, 'w') as f:
        f.write(html_content)
    print(f"HTML report saved to: {output_file}")

def main():
    parser = argparse.ArgumentParser(description='Simple EDI 271 Parser')
    parser.add_argument('input_file', help='Path to EDI 271 file')
    parser.add_argument('--html-output', help='Output path for HTML report')
    parser.add_argument('--json-output', help='Output path for JSON data')
    
    args = parser.parse_args()
    
    try:
        print(f"Parsing EDI file: {args.input_file}")
        parser_obj = SimpleEDI271Parser()
        data = parser_obj.parse_file(args.input_file)
        
        if args.html_output:
            generate_html_report(data, args.html_output)
        
        if args.json_output:
            os.makedirs(os.path.dirname(args.json_output), exist_ok=True)
            with open(args.json_output, 'w') as f:
                json.dump(asdict(data), f, indent=2)
            print(f"JSON saved to: {args.json_output}")
        
        print("\n=== PARSING RESULTS ===")
        print(f"Subscriber: {data.subscriber_name}")
        print(f"Payer: {data.payer_name}")
        print(f"Plan: {data.plan_name}")
        print(f"Transaction ID: {data.transaction_id}")
        
    except Exception as e:
        print(f"Error: {e}")
        return 1

if __name__ == "__main__":
    main()