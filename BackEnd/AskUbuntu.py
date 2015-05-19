#!/usr/bin/env python2.7
####################################################
# Columbia University
# Department of Electrical Engineering
# Big Data - Final Project
# Spring 2015
# Andre Cunha (adc2171)
####################################################
from pyspark import SparkContext
import re

# Create A Standalone App in Python
sc = SparkContext("local", "YAHOO-SD")

class MapReduce():
  '''
  MapReduce Class
  '''
  def __init__(self, f):
    # Load Posts.XML File
    self.lines = sc.textFile(f)
    self.RDD_CLEAN = None
    
  def Map(self):
    '''
    Map task:
    The mapper algorithm is responsible to take the raw text of AskUbuntu $XML$ dataset, organize the data, and convert it to key/value pairs. 
    '''
    # All the Post have PostTypeId="1"
    # Comments have PostTypeId="2"
    self.RDD_CLEAN = self.lines.filter(lambda x: 'PostTypeId="1"' in x)
    self.RDD_CLEAN = self.RDD_CLEAN.map(FilterRDD)
    #print self.RDD_CLEAN.take(2)
    
  def Text2Id(self):
    '''
    Text2Id is part of the mapper algorithm. Here it calculate the rate metric and create the (key,value) pairs
    '''
    self.RDD_CLEAN = self.RDD_CLEAN.flatMap(ConvertText)
    print self.RDD_CLEAN.take(4)
    
  def Reduce(self):
    '''
    Reduce task
    The reducer step will group all keys, and create a list with their respective values. This will reduce the output to a list of unique keys, each with a value corresponding to different questions.
    The result will be export to a MySql Data
    '''
    self.RDD_CLEAN = self.RDD_CLEAN.groupByKey()
    self.RDD_CLEAN = self.RDD_CLEAN.map(lambda x : [x[0], list(x[1])] )
    #self.RDD_CLEAN = self.RDD_CLEAN.map()
    # Save the results on a txt to import to a Mysql Database
    f = open("Mysql_input.txt","w")
    f.write(self.RDD_CLEAN.collect())
    #a = self.RDD_CLEAN.collect()
    #print a

def CompareValue(x):
  k,r = x
  # Select the 3 best POSTS for each TagID
  if (len(r) < 3):
    return k, sorted(r,reverse=True, key=lambda h: h[1])
  else:
    return (k, sorted(r[:3],reverse=True, key=lambda h: h[1]))
  

def FilterRDD(x):
  r = re.search(r'Id="(.*)" PostTypeId="1" .* Score="(.*)" ViewCount="(.*)" B.* Title="(.*)" Tags="(.*)" AnswerCount="(.*)" CommentCount="(.*)" FavoriteCount="(\d*)"', x)
  
  if (r is None):
    return [0,0,0,0,0,0,0,0]
  
  return r.groups()

def ConvertText(x):
  Id,Score,ViewCount,Title,Tags,AnswerCount,CommentCount,FavoriteCount = x
  
  Value = float(Score)*1.5 + float(ViewCount)/1000 + float(AnswerCount)*3 + float(CommentCount)*2.3 + float(FavoriteCount)*3.3
  Tags = str(Tags).replace("&lt;", "")
  Tags = Tags.split("&gt;")
  l = []
  for i in Tags:
    if i != "":
      l.append([i, [Id,Value,Title]])
      
  return l

class Tag():
  '''
  Class Tag is responsible to process and clean the Tag.xml dataset
  '''
  def __init__(self, f):
    '''
    An RDD in Spark is simply an immutable distributed collection of objects. Each RDD is split into multiple partitions, which may be computed on different nodes of the cluster. RDDs can contain any type of Python, Java, or Scala objects, including user- defined classes.
    
    There are two ways to creat a RDD:
    List
    SparkContext.textFile()
    '''
    
    # Load TAGS.XML File
    self.lines = sc.textFile(f)
    
  def Parser(self):
    '''
    Parser function responsible to read Xml data. It uses Regular Expression do it.
    '''
    RDD_CLEAN = self.lines.filter(lambda x: 'row' in x)
    RDD_CLEAN = self.lines.flatMap(lambda x: [(re.search(r'Id="(.*)" TagName="(.*)" Count="(.*)" E', x)).groups()] )
    #print RDD_CLEAN.take(2)

def main():
  # First, create an instance of Tag
  t = Tag("file:///home/azevedo/Documentos/Columbia/Courses/AdvBigData/Project/Tags.xml")
  # Parse the results
  t.Parser()
  
  
  p = MapReduce("file:///home/azevedo/Documentos/Columbia/Courses/AdvBigData/Project/Posts.xml")
  p.Map()
  p.Text2Id()
  p.Reduce()

if __name__ == '__main__':
  main()