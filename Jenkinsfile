pipeline {
    agent any

    environment {
        REPO_NAME       = 'etl-pms-customer'
        IMAGE_BASE      = '<YOUR_REGISTRY>/docker-registry   // [SCRUBBED] replace with your Docker registry'
        IMAGE_NAME      = "${IMAGE_BASE}/${REPO_NAME}"
        IMAGE_TAG       = "${IMAGE_NAME}:${env.BRANCH_NAME}-${env.GIT_COMMIT.take(8)}"
        PREFECT_API_URL = 'http://<PREFECT_SERVER_IP>:4200/api   // [SCRUBBED] replace with your Prefect server IP'
        SECRET_PROJECT  = 'your-gcp-project-id'   // [SCRUBBED] replace with your GCP project ID
    }

    stages {
        stage('Who Am I') {
            steps { sh 'whoami && pwd' }
        }

        stage('Code Analysis (SonarQube)') {
            when { anyOf { branch 'develop'; branch 'main'; buildingTag() } }
            steps {
                withSonarQubeEnv('SonarQube') {
                    sh 'mvn sonar:sonar -f pom.xml'
                }
            }
        }

        stage('Validate Branch') {
            when { not { anyOf { branch 'develop'; branch 'main' } } }
            steps { echo "Skipping build for branch ${env.BRANCH_NAME}" }
        }

        stage('Build Docker Image') {
            when { anyOf { branch 'develop'; branch 'main' } }
            steps {
                sh """
                    docker build -t ${IMAGE_TAG} .
                """
            }
        }

        stage('Push to Artifact Registry') {
            when { anyOf { branch 'develop'; branch 'main' } }
            steps {
                sh "docker push ${IMAGE_TAG}"
            }
        }

        stage('Deploy Prefect v3') {
            when { anyOf { branch 'develop'; branch 'main' } }
            steps {
                withCredentials([file(credentialsId: 'gcp-sa-key', variable: 'GCP_SA_KEY')]) {
                    sh """
                        docker run --rm \\
                            -e CI_COMMIT_BRANCH=${env.BRANCH_NAME} \\
                            -e IMAGE_TAG=${IMAGE_TAG} \\
                            -e PREFECT_API_URL=${PREFECT_API_URL} \\
                            -e PREFECT_WORK_POOL=kubernetes-pool \\
                            -e PREFECT_WORK_QUEUE=default \\
                            -e SECRET_PROJECT_ID=${SECRET_PROJECT} \\
                            -e GOOGLE_APPLICATION_CREDENTIALS=/tmp/sa-key.json \\
                            -v \${GCP_SA_KEY}:/tmp/sa-key.json:ro \\
                            ${IMAGE_TAG} \\
                            python deploy.py
                    """
                }
            }
        }

        stage('Remove Local Image') {
            when { anyOf { branch 'develop'; branch 'main' } }
            steps {
                sh "docker rmi ${IMAGE_TAG} || true"
            }
        }
    }

    post {
        failure { echo "Pipeline failed for ${env.BRANCH_NAME}" }
    }
}
