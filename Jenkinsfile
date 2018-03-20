pipeline {
    agen any
    stages {
        stage('Build') {
            steps {
                python /var/jenkins_home/scripts/aws-text-mining/main.py --bucket "${S3_BUCKET}" --inputPrefix "${INPUT_PREFIX}" --outputPrefix "${OUTPUT_PREFIX}"
            }
        }
    }
