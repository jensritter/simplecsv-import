@Library("jens-pipeline-lib") _
pipeline {
//    agent any
    agent { label 'linux' }
    triggers {
        snapshotDependencies()
        pollSCM '@hourly'
    }

    stages {
        stage("build") {
            steps {
                withMaven(
                        jdk: "${env.DEFAULT_JDK}",
                        maven: "${env.DEFAULT_MAVEN}",
                        mavenLocalRepo: '.repository'
                ) {
                    jensCommand "mvn clean deploy"
                    jensCommand "mvn sonar:sonar"
                }
            }
            post {
                cleanup {
                    dir('.repository') { deleteDir() }
                }
                always {
                    recordIssues(tools: [java(), mavenConsole(), /* kotlin() */])
                }
            }
        }
    }
    post {
        always {
            jabberNotify notificationStrategy: 'new failure and fixed', targets: 'ritter@chat.mitegro.net'
        }
    }
}
