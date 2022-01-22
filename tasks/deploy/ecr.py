import os

from core.aws import ECROperator


def register():
    deploy = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(deploy)
    ecr = ECROperator('tasks')
    image = ecr.execute(project_root)
    print(image)


if __name__ == '__main__':
    register()
