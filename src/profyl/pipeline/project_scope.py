def find_project(projects: dict[dict], project_name: str) -> dict | None:
    return projects.get(project_name)