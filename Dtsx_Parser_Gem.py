import xml.etree.ElementTree as ET
import re

def parse_dtsx_file(file_path):
    try:
        # Parse the dtsx file
        tree = ET.parse(file_path)
        root = tree.getroot()

        # Find the namespace
        namespace = re.match(r'\{(.*)\}', root.tag)[1]

        # 1. Connection Managers
        connection_managers = root.findall(f'.//{namespace}ConnectionManager')
        print("Connection Managers:")
        for connection_manager in connection_managers:
            connection_string = connection_manager.find(f'.//{namespace}ConnectionString').text
            server = re.search(r'server=(.*);', connection_string, re.IGNORECASE).group(1)
            print(f"  - Connection String: {connection_string}")
            print(f"  - Server: {server}")

        # 2. Package Configurations
        package_configurations = root.findall(f'.//{namespace}PackageConfiguration')
        print("\nPackage Configurations:")
        for package_configuration in package_configurations:
            configuration_name = package_configuration.find(f'.//{namespace}ConfigurationName').text
            print(f"  - {configuration_name}")

        # 3. DFTs
        data_flow_tasks = root.findall(f'.//{namespace}DataFlowTask')
        print("\nData Flow Tasks (DFTs):")
        for data_flow_task in data_flow_tasks:
            source_component = data_flow_task.find(f'.//{namespace}SourceComponent')
            destination_component = data_flow_task.find(f'.//{namespace}DestinationComponent')
            if source_component and destination_component:
                source_name = source_component.find(f'.//{namespace}ComponentName').text
                destination_name = destination_component.find(f'.//{namespace}ComponentName').text
                column_mappings = data_flow_task.findall(f'.//{namespace}ColumnMapping')
                print(f"  - Source: {source_name}")
                print(f"  - Destination: {destination_name}")
                print("  - Column Mappings:")
                for column_mapping in column_mappings:
                    source_column = column_mapping.find(f'.//{namespace}SourceColumn').text
                    destination_column = column_mapping.find(f'.//{namespace}DestinationColumn').text
                    print(f"    - {source_column} -> {destination_column}")

        # 4. SQL Execute Statements
        sql_execute_tasks = root.findall(f'.//{namespace}ExecuteSQLTask')
        print("\nSQL Execute Statements:")
        for sql_execute_task in sql_execute_tasks:
            sql_statement = sql_execute_task.find(f'.//{namespace}SQLStatement').text
            print(f"  - {sql_statement}")

    except Exception as e:
        print(f"Error parsing dtsx file: {e}")

if __name__ == "__main__":
    file_path = "your_dtsx_file.dtsx"  # Replace with your dtsx file path
    parse_dtsx_file(file_path)
