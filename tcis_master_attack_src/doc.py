import pandas as pd
from owlready2 import *
import os


os.chdir('tcis_master_attack_src')
capec = pd.read_csv('./attack_files/3000.csv')
capec = capec.fillna(' ')

new_names = ['ID', 'Name', 'Abstraction', 'Status', 'Description',
       'Alternate Terms', 'Likelihood Of Attack', 'Typical Severity',
       'Related Attack Patterns', 'Execution Flow', 'Prerequisites',
       'Skills Required', 'Resources Required', 'Indicators', 'Consequences',
       'Mitigations', 'Example Instances', 'Related Weaknesses',
       'Taxonomy Mappings', 'Notes']
capec = capec.rename({'Notes': 'delete me', 'Taxonomy Mappings': 'Notes', 'Related Weaknesses':'Taxonomy Mappings' , 'Example Instances':'Related Weaknesses' , 'Mitigations':'Example Instances' , 'Consequences':'Mitigations' , 'Indicators':'Consequences' , 'Resources Required':'Indicators' , 'Skills Required':'Resources Required' , 'Prerequisites':'Skills Required' , 'Execution Flow':'Prerequisites' , 'Related Attack Patterns':'Execution Flow' , 'Typical Severity':'Related Attack Patterns' , 'Likelihood Of Attack':'Typical Severity' , 'Alternate Terms':'Likelihood Of Attack' , 'Description':'Alternate Terms' , 'Status':'Description', 'Abstraction':'Status', 'Name':'Abstraction', 'ID':'Name', 0:'id'}, axis=1)
capec = capec.drop(columns=['delete me'])
capec.reset_index(level=0, inplace=True)

onto = get_ontology('attack_files/capec_ontology.owl#')
capec_list = [212,185,216,116,549]

with onto:
    #Create ontology structure
    class Attack(Thing):
      def get_attack(self):
        return self.name
    class ID(Attack):
      def get_id(self):
        return self.id
    class Name(Attack):
      def get_name(self):
        return self.name
    class RelatedWeaknesses(Attack):
      def get_weakness(self):
        return self.vulnerability
    class Mitigations(Attack):
      def get_countermeasure(self):
        return self.countermeasure
    class Prerequisites(Attack):
      def get_prerequisite(self):
        return self.prerequisite
    class SkillsRequired(Attack):
      def get_skill(self):
        return self.skill
    class ResourcesRequired(Attack):
      def get_resource(self):
        return self.resource
    class ExecutionFlow(Attack):
      def get_method(self):
        return self.method
    class Consequences(Attack):
      def get_consequence(self):
        return self.consequence
    class TypicalSeverity(Attack):
      def get_severity(self):
        return self.severity
    class LikelihoodOfAttack(Attack):
      def get_likelihood(self):
        return self.likelihood
# Create relationships
    class has_id(Attack >> ID):
      python_name = "id"
    class has_name(Attack >> Name):
      python_name = "name"
    class has_vulnerability(Attack >> RelatedWeaknesses):
      python_name = "vulnerability"
    class has_countermeasures(Attack >> Mitigations):
      python_name = "countermeasure"
    class work_against(Mitigations >> RelatedWeaknesses):pass
    class has_prerequisites(Attack >> Prerequisites):
      python_name = "prerequisite"
    class need_skills(Attack >> SkillsRequired):
      python_name = "skills"
    class need_resources(Attack >> ResourcesRequired):
      python_name = "resource"
    class has_method(Attack >> ExecutionFlow):
      python_name = "method"
    class has_consequences(Attack >> Consequences):
      python_name = "consequence"
    class has_severity(Attack >> TypicalSeverity):
      python_name = "severity"
    class has_likelihood(Attack >> LikelihoodOfAttack):
      python_name = "likelihood"
#Create indivudials for an attack
    abuse = Attack("abuse")
    invoke = Attack("invoke")
    exfiltration = Attack("exfiltration")
    malicious_copy = Attack("malicious_copy")
    ransomware = Attack("ransomware")

    attack_list = [abuse, invoke, exfiltration,malicious_copy,ransomware]

    for i,j in zip(attack_list,capec_list):
        i.has_id = capec[capec['index']==j]['index']
        i.has_name = capec[capec['index']==j]['Name']
        i.has_vulnerability = capec[capec['index']==j]['Related Weaknesses']
        i.has_countermeasures = capec[capec['index']==j]['Mitigations']
        # i.work_against = capec[capec['index']==j]['Related Weaknesses']
        i.has_prerequisites = capec[capec['index']==j]['Prerequisites']
        i.need_skills = capec[capec['index']==j]['Skills Required']
        i.need_resources = capec[capec['index']==j]['Resources Required']
        i.has_method = capec[capec['index']==j]['Execution Flow']
        i.has_consequences = capec[capec['index']==j]['Consequences']
        i.has_severity = capec[capec['index']==j]['Typical Severity']
        i.has_likelihood = capec[capec['index']==j]['Likelihood Of Attack']

onto.save('./ontology/capec_ontology.owl')

def ontology_display(attack):
  if attack == 'abuse':
      attack_display(abuse)
  elif attack == 'invoke':
      attack_display(invoke)
  elif attack == 'exfiltration':
      attack_display(exfiltration)
  elif attack == 'malicious_copy':
      attack_display(malicious_copy)
  elif attack == 'ransomware':
      attack_display(ransomware)

def attack_display(attack):
  print('CAPEC id:', attack.has_id.iloc[0])
  print('Name: ', attack.has_name.iloc[0])
  print('Vulnerabilties: ', attack.has_vulnerability.iloc[0])
  print('Countermeasures: ', attack.has_countermeasures.iloc[0])
  # print(attack.work_against.iloc[0])
  print('Prerequisires: ', attack.has_prerequisites.iloc[0])
  print('Skills needed: ', attack.need_skills.iloc[0])
  print('Resources needed: ', attack.need_resources.iloc[0])
  print('Attack method: ', attack.has_method.iloc[0])
  print('Consequences: ', attack.has_consequences.iloc[0])
  print('Severity: ', attack.has_severity.iloc[0])
  print('Attack likelihood: ', attack.has_likelihood.iloc[0]) 