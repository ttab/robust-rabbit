@Library("common") _

node {

  stage('checkout') {
    checkout scm
    sh "git clean -d -f -x"
  }

  docker.image('node:erbium').inside {
    stage('yarn install') {
      sh 'yarn install'
    }

    stage('jest') {
      sh 'node_modules/.bin/jest --reporters=jest-junit'
      junit "junit.xml"
    }
  }

}
