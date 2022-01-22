# Tasks

이 디렉터리 전체를 ECR 배포 하여 원하는 스크립트를 실행할 수 있습니다.

## ECR 배포 방법
AWS 프로그래밍 접근이 가능하고 tasks 라는 ECR 레포지토리가 AWS 계정에 있어야 합니다

```
python deploy/ecr.py
```

## ECS 배포 방법

1. src 디렉터리 내의 파일을 참고해서 작업 정의(task_definition) 를 등록합니다
   1. 코드를 보면 Dockerfile 을 새로 만들어 다른 프로그래밍 언어의 코드도 배포가 가능합니다
2. 작업 정의를 기반으로 작업을 실행합니다
