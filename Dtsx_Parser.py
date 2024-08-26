import xml.etree.ElementTree as ET

def parse_dtsx(file_path):
    tree = ET.parse(file_path)
    root = tree.getroot()

    namespace = {'': 'www.microsoft.com/SqlServer/Dts'}

    # 1. Connection Managers
    connection_managers = root.findall('.//ConnectionManagers/ConnectionManager', namespace)
    connection_info = []
    for cm in connection_managers:
        conn_string = cm.attrib.get('ConnectionString', 'N/A')
        server = "N/A"
        if 'Data Source=' in conn_string:
            server = conn_string.split('Data Source=')[1].split(';')[0]
        connection_info.append({
            'name': cm.attrib.get('Name'),
            'connection_string': conn_string,
            'server': server
        })

    # 2. Package Configurations
    package_configs = root.findall('.//Configurations/Configuration', namespace)
    package_config_info = [pc.attrib.get('ConfigurationString', 'N/A') for pc in package_configs]

    # 3. Data Flow Tasks (DFTs)
    dfts = root.findall('.//Executable[@DTS:ExecutableType="SSIS.Pipeline.2"]', namespace)
    dft_info = []
    for dft in dfts:
        dft_name = dft.attrib.get('DTS:Name', 'N/A')
        # Find components (sources, transformations, destinations) within the DFT
        components = dft.findall('.//Component', namespace)
        for comp in components:
            comp_name = comp.attrib.get('Name', 'N/A')
            comp_type = comp.attrib.get('ComponentClassID', 'N/A')
            if "source" in comp_type.lower():
                src_name = comp_name
            elif "destination" in comp_type.lower():
                dst_name = comp_name
            # Get column mappings (InputColumn & OutputColumn)
            mappings = []
            for mapping in comp.findall('.//InputColumn', namespace):
                output_column = mapping.attrib.get('OutputColumnLineageID', 'N/A')
                input_column = mapping.attrib.get('Name', 'N/A')
                mappings.append({'input': input_column, 'output': output_column})
            dft_info.append({
                'dft_name': dft_name,
                'from': src_name if 'src_name' in locals() else 'N/A',
                'to': dst_name if 'dst_name' in locals() else 'N/A',
                'mappings': mappings
            })

    # 4. SQL Execute Tasks
    sql_tasks = root.findall('.//Executable[@DTS:ExecutableType="Microsoft.SqlServer.Dts.Tasks.ExecuteSQLTask.ExecuteSQLTask, Microsoft.SqlServer.SQLTask"]', namespace)
    sql_task_info = [{'name': task.attrib.get('DTS:Name', 'N/A'), 'sql': task.find('.//SQLTask:SqlStatementSource', namespace).text if task.find('.//SQLTask:SqlStatementSource', namespace) is not None else 'N/A'} for task in sql_tasks]

    return {
        'connection_managers': connection_info,
        'package_configurations': package_config_info,
        'data_flow_tasks': dft_info,
        'sql_execute_tasks': sql_task_info
    }

if __name__ == "__main__":
    file_path = 'path/to/your/package.dtsx'
    result = parse_dtsx(file_path)
    
    print("Connection Managers:")
    for cm in result['connection_managers']:
        print(f"Name: {cm['name']}, Connection String: {cm['connection_string']}, Server: {cm['server']}")

    print("\nPackage Configurations:")
    for pc in result['package_configurations']:
        print(f"Configuration: {pc}")

    print("\nData Flow Tasks (DFTs):")
    for dft in result['data_flow_tasks']:
        print(f"DFT Name: {dft['dft_name']}, From: {dft['from']}, To: {dft['to']}")
        for mapping in dft['mappings']:
            print(f"    Mapping: Input: {mapping['input']} -> Output: {mapping['output']}")

    print("\nSQL Execute Tasks:")
    for task in result['sql_execute_tasks']:
        print(f"Task Name: {task['name']}, SQL: {task['sql']}")
