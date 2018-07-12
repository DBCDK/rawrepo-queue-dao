#!groovy

def workerNode = "devel8"

pipeline {
	agent {label workerNode}
	tools {
		maven "Maven 3"
	}
	triggers {
		pollSCM("H/03 * * * *")
	}
	options {
		timestamps()
	}
	stages {
		stage("veriMaven build") {
			steps {
				sh "mvn clean install pmd:pmd findbugs:findbugs"
			}
		}
		stage("Check warnings") {
			steps {
				warnings consoleParsers: [
					[parserName: "Java Compiler (javac)"],
					[parserName: "JavaDoc Tool"]],
					unstableTotalAll: "0",
					failedTotalAll: "0"
			}
		}
		stage("PMD") {
			steps {
				step([$class: 'hudson.plugins.pmd.PmdPublisher',
					  pattern: '**/target/pmd.xml',
					  unstableTotalAll: "0",
					  failedTotalAll: "0"])
			}
		}
		stage("Install artifact") {
            when {
                expression { env.BRANCH_NAME == master }
            }
            steps {
                sh "mvn install"
            }
		}
    }
}
