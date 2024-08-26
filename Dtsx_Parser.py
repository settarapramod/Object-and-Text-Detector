
import xml.etree.ElementTree as ET

def parse_dtsx(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Namespace dictionary to handle XML namespaces in the .dtsx file
        ns = {'dts': 'www.microsoft.com/SqlServer/Dts'}

        # 1. How many connections managers are used? What is the connection string? What is the server it is trying to connect?
        connection_managers = root.findall(".//dts:ConnectionManager", ns)
        print(f"Total Connection Managers: {len(connection_managers)}")
        for conn in connection_managers:
            conn_name = conn.get('NAME')
            connection_string = conn.find(".//dts:ConnectionString", ns).text if conn.find(".//dts:ConnectionString", ns) is not None else 'N/A'
            server_name = connection_string.split(';')[0] if connection_string != 'N/A' else 'N/A'
            print(f"Connection Manager: {conn_name}")
            print(f"Connection String: {connection_string}")
            print(f"Server Name: {server_name}")
            print("")

        # 2. How many package configurations are present and what are they?
        package_configurations = root.findall(".//dts:Configuration", ns)
        print(f"Total Package Configurations: {len(package_configurations)}")
        for config in package_configurations:
            config_name = config.get('NAME')
            config_type = config.get('CONFIGURATIONTYPE')
            print(f"Package Configuration: {config_name}, Type: {config_type}")
            print("")

        # 3. How many DFTs are there and from where to where data is being moved. What are the column mappings done.
        data_flows = root.findall(".//dts:Executable[@DTSTask=\"{0F7D89E4-35EF-4C65-AB22-FF7EA257D11A}\"]", ns)
        print(f"Total Data Flow Tasks (DFTs): {len(data_flows)}")
        for dft in data_flows:
            dft_name = dft.get('NAME')
            print(f"Data Flow Task: {dft_name}")
            sources = dft.findall(".//dts:component[@componentClassID=\"{7A62D3E3-38FD-49B4-AFF3-FE1B361B81AC}\"]", ns)  # OLE DB Source
            destinations = dft.findall(".//dts:component[@componentClassID=\"{5A5CBE2D-8A73-4991-92A0-44D876233989}\"]", ns)  # OLE DB Destination
            for source in sources:
                source_name = source.get('name')
                print(f"Source Component: {source_name}")
            for destination in destinations:
                destination_name = destination.get('name')
                print(f"Destination Component: {destination_name}")
                mappings = destination.findall(".//dts:outputColumn/@name", ns)
                for mapping in mappings:
                    print(f"Column Mapping: {mapping.get('name')}")
            print("")

        # 4. How many SQL execute statements are being used and what are they
        sql_tasks = root.findall(".//dts:Executable[@DTSTask=\"{E0C0F157-96A5-44ED-AB6C-DEC2C2909150}\"]", ns)
        print(f"Total SQL Execute Tasks: {len(sql_tasks)}")
        for sql_task in sql_tasks:
            sql_name = sql_task.get('NAME')
            sql_statement = sql_task.find(".//dts:SqlStatementSource", ns).text
            print(f"SQL Task: {sql_name}")
            print(f"SQL Statement: {sql_statement}")
            print("")

    except ET.ParseError as e:
        print(f"Error parsing the DTSX file: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == "__main__":
    # Replace 'your_file.dtsx' with the path to your .dtsx file
    file_path = 'your_file.dtsx'
    parse_dtsx(file_path)
```

### Explanation:

1. **Connection Managers:**
   - The script locates all connection managers and retrieves the connection string and server name.

2. **Package Configurations:**
   - It counts and lists all package configurations.

3. **Data Flow Tasks (DFTs):**
   - It counts the number of DFTs, identifies the source and destination components within the DFT, and lists column mappings.

4. **SQL Execute Statements:**
   - It identifies SQL execute tasks and extracts the SQL statements.

### Error Handling:
- The script handles XML parsing errors and general exceptions to ensure that issues like missing elements or incorrect formats do not cause the script to fail unexpectedly.

### Running the Script:
- Replace `'your_file.dtsx'` with the actual path to your `.dtsx` file when running the script.

Let me know if you need any further assistance or modifications!
