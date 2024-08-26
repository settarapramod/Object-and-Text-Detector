import xml.etree.ElementTree as ET

def parse_dtsx(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Namespace dictionary to handle XML namespaces in the .dtsx file
        ns = {'dts': 'www.microsoft.com/SqlServer/Dts/Tasks'}

        # 1. How many connections managers are used? What is the connection string? What is the server it is trying to connect?
        connection_managers = root.findall(".//DTS:ConnectionManager", ns)
        print(f"Total Connection Managers: {len(connection_managers)}")
        for conn in connection_managers:
            conn_name = conn.get('DTS:Name')
            connection_string = conn.find(".//DTS:ObjectData//DTS:ConnectionManager", ns).get('DTS:ConnectionString') if conn.find(".//DTS:ObjectData//DTS:ConnectionManager", ns) is not None else 'N/A'
            server_name = connection_string.split(';')[0] if connection_string != 'N/A' else 'N/A'
            print(f"Connection Manager: {conn_name}")
            print(f"Connection String: {connection_string}")
            print(f"Server Name: {server_name}")
            print("")

        # 2. How many package configurations are present and what are they?
        package_configurations = root.findall(".//DTS:Configuration", ns)
        print(f"Total Package Configurations: {len(package_configurations)}")
        for config in package_configurations:
            config_name = config.get('DTS:ConfigurationString')
            config_type = config.get('DTS:ConfigurationType')
            print(f"Package Configuration: {config_name}, Type: {config_type}")
            print("")

        # 3. How many DFTs are there and from where to where data is being moved. What are the column mappings done.
        data_flows = root.findall(".//DTS:Executable[@DTS:ExecutableType='SSIS.Pipeline.2']", ns)
        print(f"Total Data Flow Tasks (DFTs): {len(data_flows)}")
        for dft in data_flows:
            dft_name = dft.get('DTS:ObjectName')
            print(f"Data Flow Task: {dft_name}")
            sources = dft.findall(".//component[@componentClassID='Microsoft.OLEDBSource']", ns)
            destinations = dft.findall(".//component[@componentClassID='Microsoft.OLEDBDestination']", ns)
            for source in sources:
                source_name = source.get('name')
                print(f"Source Component: {source_name}")
            for destination in destinations:
                destination_name = destination.get('name')
                print(f"Destination Component: {destination_name}")
                mappings = destination.findall(".//inputColumn", ns)
                for mapping in mappings:
                    input_column_name = mapping.get('name')
                    print(f"Column Mapping: {input_column_name}")
            print("")

        # 4. How many SQL execute statements are being used and what are they
        sql_tasks = root.findall(".//DTS:Executable[@DTS:ExecutableType='SSIS.ExecuteSQLTask.2']", ns)
        print(f"Total SQL Execute Tasks: {len(sql_tasks)}")
        for sql_task in sql_tasks:
            sql_name = sql_task.get('DTS:ObjectName')
            sql_statement = sql_task.find(".//DTS:SqlStatementSource", ns).text
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
