import xml.etree.ElementTree as ET

def get_namespace(element):
    # Extract namespace from the element tag (format: {namespace}tag)
    if element.tag.startswith("{"):
        return element.tag.split("}")[0][1:]
    return ""

def parse_dtsx(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Detect the namespace dynamically
        namespace = get_namespace(root)
        ns = {'dts': namespace}

        # 1. How many connections managers are used? What is the connection string? What is the server it is trying to connect?
        connection_managers = root.findall(".//dts:ConnectionManager", ns)
        print(f"Total Connection Managers: {len(connection_managers)}")
        for conn in connection_managers:
            conn_name = conn.get(f'{{{namespace}}}Name')
            connection_string_element = conn.find(".//dts:ConnectionString", ns)
            connection_string = connection_string_element.text if connection_string_element is not None else 'N/A'
            server_name = connection_string.split(';')[0] if connection_string != 'N/A' else 'N/A'
            print(f"Connection Manager: {conn_name}")
            print(f"Connection String: {connection_string}")
            print(f"Server Name: {server_name}")
            print("")

        # 2. How many package configurations are present and what are they?
        package_configurations = root.findall(".//dts:Configuration", ns)
        print(f"Total Package Configurations: {len(package_configurations)}")
        for config in package_configurations:
            config_name = config.get(f'{{{namespace}}}ConfigurationString')
            config_type = config.get(f'{{{namespace}}}ConfigurationType')
            print(f"Package Configuration: {config_name}, Type: {config_type}")
            print("")

        # 3. How many DFTs are there and from where to where data is being moved. What are the column mappings done.
        data_flows = root.findall(".//dts:Executable[@dts:ExecutableType='SSIS.Pipeline.2']", ns)
        print(f"Total Data Flow Tasks (DFTs): {len(data_flows)}")
        for dft in data_flows:
            dft_name = dft.get(f'{{{namespace}}}ObjectName')
            print(f"Data Flow Task: {dft_name}")
            sources = dft.findall(".//dts:component[@componentClassID='Microsoft.OLEDBSource']", ns)
            destinations = dft.findall(".//dts:component[@componentClassID='Microsoft.OLEDBDestination']", ns)
            for source in sources:
                source_name = source.get('name')
                print(f"Source Component: {source_name}")
            for destination in destinations:
                destination_name = destination.get('name')
                print(f"Destination Component: {destination_name}")
                mappings = destination.findall(".//dts:inputColumn", ns)
                for mapping in mappings:
                    input_column_name = mapping.get('name')
                    print(f"Column Mapping: {input_column_name}")
            print("")

        # 4. How many SQL execute statements are being used and what are they
        sql_tasks = root.findall(".//dts:Executable[@dts:ExecutableType='SSIS.ExecuteSQLTask.2']", ns)
        print(f"Total SQL Execute Tasks: {len(sql_tasks)}")
        for sql_task in sql_tasks:
            sql_name = sql_task.get(f'{{{namespace}}}ObjectName')
            sql_statement_element = sql_task.find(".//dts:SqlStatementSource", ns)
            sql_statement = sql_statement_element.text if sql_statement_element is not None else 'N/A'
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
