def agentLabel = [:]
        agentLabel["development"] = "imf-jenkins-np-slave"
        agentLabel["staging"] = "imf-jenkins-np-slave"
        agentLabel["master"] = "imf-jenkins-prod-slave "

def projectRepo = [:]
    projectRepo["key"] = "IMFDE"
    projectRepo["repoName"] = "spark-salesforce"
    projectRepo["url"] = "https://bitbucket-aws.itss.vic.gov.au"

pipeline {
    environment {
        GIT_COMMITTER_EMAIL = sh(
                script: "git --no-pager show -s --format='%ae'",
                returnStdout: true
        ).trim()
        GIT_COMMITTER_NAME = sh(
                script: "git --no-pager show -s --format='%an'",
                returnStdout: true
        ).trim()
        GIT_REMOTE_URL = sh(
                script: "git config --get remote.origin.url",
                        returnStdout: true
                ).trim()
    }
    agent {
        label agentLabel[BRANCH_NAME]
    }
    options {
        disableConcurrentBuilds()
    }

    stages {
            stage('Prepare Environment') {
            steps {
                sh 'echo Building ${BRANCH_NAME}...'
                withCredentials([usernamePassword(credentialsId: 'scm_credentials', usernameVariable: 'GIT_USER', passwordVariable: 'GIT_PASSWORD')]) {
                sh (
                script :"git config --local credential.helper 'cache --timeout=3600'"
                )
                }
                git credentialsId: 'scm_credentials', url: "${env.GIT_REMOTE_URL}"
   				sh ( "git show-ref refs/heads/${BRANCH_NAME} || git checkout --track origin/${BRANCH_NAME} || true" )
                sh (
                script: "git branch --set-upstream-to=origin/${BRANCH_NAME} ${BRANCH_NAME} && git checkout ${BRANCH_NAME} && git pull && git fetch --tags -f"
                )

            }
        }

        stage('Development Build') {
            when {
                branch 'development'
            }
            steps {
                  withCredentials([usernamePassword(credentialsId: 'artifactory_development', usernameVariable: 'ARTIFACTORY_USER', passwordVariable: 'ARTIFACTORY_PASSWORD')]) {
                sh "gradle clean reportScoverage jar shadowJar scaladoc"
                }
            }
        }

        stage('Code Promotion to staging(create PR)') {
            when {
                branch 'development'
            }
            steps {
                sh "git push origin --tags -f"
                withCredentials([usernamePassword(credentialsId: 'scm_credentials', usernameVariable: 'GIT_USER', passwordVariable: 'GIT_PASSWORD')]) {
                sh """
                curl -u ${GIT_USER}:${GIT_PASSWORD} -H "Content-Type: application/json" -k ${projectRepo['url']}/rest/api/1.0/projects/${projectRepo['key']}/repos/${projectRepo['repoName']}/pull-requests -X POST --data '
                {
                   "title": "Pull request created by jenkins by build No: ${BUILD_NUMBER} url : ${BUILD_URL}",
                   "description": "pull request is created by jenkins for the build:  ${BUILD_URL}",
                    "fromRef": {
                        "id": "refs/heads/development",
                        "repository": {
                            "slug": "${projectRepo['repoName']}",
                            "name": null,
                            "project": {
                                "key": "${projectRepo['key']}"
                            }
                        }
                    },
                    "toRef": {
                        "id": "refs/heads/staging",
                        "repository": {
                            "slug": "${projectRepo['repoName']}",
                            "name": null,
                            "project": {
                                "key": "${projectRepo['key']}"
                            }
                        }
                    }
                }' || true
                """
                }
            }
        }
        stage('Staging Build') {
            when {
                branch 'staging'
            }
            steps {
            withCredentials([usernamePassword(credentialsId: 'artifactory_staging', usernameVariable: 'ARTIFACTORY_USER', passwordVariable: 'ARTIFACTORY_PASSWORD')]) {
               sh "gradle clean reportScoverage jar shadowJar scaladoc artifactoryPublish"
            }
            }
        }

        stage('Push Staging Tag to Staging Branch') {
            when {
                branch 'staging'
            }
            steps {
                sh "git push origin --tags -f"
            }
        }

        stage('Create Staging to master pull request') {
            when {
                branch 'staging'
            }
            steps {
                script {
                    withCredentials([usernamePassword(credentialsId: 'scm_credentials', usernameVariable: 'GIT_USER', passwordVariable: 'GIT_PASSWORD')]) {
                      sh """
                      curl -u ${GIT_USER}:${GIT_PASSWORD} -H "Content-Type: application/json" -k ${projectRepo['url']}/rest/api/1.0/projects/${projectRepo['key']}/repos/${projectRepo['repoName']}/pull-requests -X POST --data '
                      {
                         "title": "Pull request created by jenkins by build No: ${BUILD_NUMBER} url : ${BUILD_URL}",
                         "description": "pull request is created by jenkins for the build:  ${BUILD_URL}",
                          "fromRef": {
                              "id": "refs/heads/staging",
                              "repository": {
                                  "slug": "${projectRepo['repoName']}",
                                  "name": null,
                                  "project": {
                                      "key": "${projectRepo['key']}"
                                  }
                              }
                          },
                          "toRef": {
                              "id": "refs/heads/master",
                              "repository": {
                                  "slug": "${projectRepo['repoName']}",
                                  "name": null,
                                  "project": {
                                      "key": "${projectRepo['key']}"
                                  }
                              }
                          }
                      }' || true
                      """
                      }
                }
            }
        }

        stage('Master Build Process ...') {
            when {
                branch 'master'
            }
            steps {
            withCredentials([usernamePassword(credentialsId: 'artifactory_master', usernameVariable: 'ARTIFACTORY_USER', passwordVariable: 'ARTIFACTORY_PASSWORD')]) {
              sh "gradle clean reportScoverage jar shadowJar scaladoc artifactoryPublish"
            }
            }
        }


        stage('Push master Tag to master Branch') {
            when {
                branch 'master'
            }
            steps {
                sh "git push origin --tags -f"
            }
        }

        stage('Deploy code into Production environment') {
            when {
                branch 'master'
            }
            steps {
                sh 'echo Code deployment goes here ...'
            }
        }
    }

    post {
        always {
            emailext body: """
                |Hi ${env.GIT_COMMITTER_NAME},

                |Build for ${env.GIT_COMMIT} in ${env.BRANCH_NAME} commenced.

                |Please go to ${env.BUILD_URL} and verify the build

                |Mail from / Build Server.
                """.stripMargin().stripIndent(),
                    subject: """Build Update for - ${JOB_NAME}  ${BUILD_DISPLAY_NAME} - ${
                        currentBuild.currentResult
                    }""", to: "${env.GIT_COMMITTER_EMAIL}"
        }
            success {
                // publish html
                publishHTML (target: [
                        allowMissing: true,
                        alwaysLinkToLastBuild: false,
                        keepAll: true,
                        reportDir: 'build/reports/scoverage',
                        reportFiles: 'index.html',
                        reportName: 'Test Coverage Report'
                ])
                publishHTML (target: [
                        allowMissing: true,
                        alwaysLinkToLastBuild: false,
                        keepAll: true,
                        reportDir: 'build/reports/tests/testScoverage',
                        reportFiles: 'index.html',
                        reportName: 'Test Summary Report'
                ])
                publishHTML (target: [
                        allowMissing: true,
                        alwaysLinkToLastBuild: false,
                        keepAll: true,
                        reportDir: 'build/docs/scaladoc',
                        reportFiles: 'index.html',
                        reportName: 'Scala Docs'
                ])
                archiveArtifacts artifacts: 'build/libs/*.jar', allowEmptyArchive: true, fingerprint: true
                junit allowEmptyResults: true,  testResults: 'build/test-results/**/*.xml'
                step([$class: 'CoberturaPublisher',
                      autoUpdateHealth: false,
                      autoUpdateStability: false,
                      coberturaReportFile: 'build/reports/scoverage/cobertura.xml',
                      failUnhealthy: false,
                      failUnstable: false,
                      maxNumberOfBuilds: 0,
                      onlyStable: false,
                      enableNewApi : true,
                      sourceEncoding: 'ASCII',
                      zoomCoverageChart: false])
            }
        }
}