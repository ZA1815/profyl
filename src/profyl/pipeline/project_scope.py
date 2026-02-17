def find_project(projects: list[dict], project_name: str) -> dict | None:
    for proj in projects:
        if proj["name"] == project_name:
            return proj
    return None