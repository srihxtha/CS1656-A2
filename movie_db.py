import sqlite3 as lite
import csv
import re
import pandas as pd
import argparse
import collections
import json
import glob
import math
import os
import requests
import string
import sqlite3
import sys
import time
import xml


class Movie_db(object):
    def __init__(self, db_name):
        # db_name: "cs1656-public.db"
        self.con = lite.connect(db_name)
        self.cur = self.con.cursor()

    # q0 is an example
    def q0(self):
        query = '''SELECT COUNT(*) FROM Actors'''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q1(self):
        query = '''
        SELECT fname, lname 
        FROM Actors as a, Movies as m80, Movies as m2000, Cast as c80, Cast as c2000
        WHERE a.aid = c80.aid AND m80.mid = c80.mid AND m80.year BETWEEN 1980 AND 1990 
        AND a.aid = c2000.aid AND m2000.mid = c2000.mid AND m2000.year >= 2000
        ORDER BY lname ASC, fname ASC

        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q2(self):
        query = ''' 
        SELECT m.title, m.year
        FROM Movies as m, (SELECT * FROM Movies as rw WHERE rw.title = 'Rogue One: A Star Wars Story') as mRW
        WHERE m.year = mRW.year AND m.rank > mRW.rank
        ORDER BY m.title ASC

        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q3(self):
        query = ''' 
        SELECT  fname, lname, mr.creds
        FROM Actors as a, (SELECT c.aid, count(role) as creds
                            FROM Movies as m, Cast as c
                            WHERE m.mid = c.mid AND m.title LIKE '%Star Wars%'
                            GROUP BY c.aid HAVING creds >= 1) mr
        WHERE a.aid = mr.aid 
        ORDER BY mr.creds DESC, a.lname ASC, a.fname ASC

        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q4(self):
        query = '''
        SELECT DISTINCT a.fname, a.lname 
        FROM Actors as a, Movies as m, Cast as c 
        WHERE a.aid = c.aid AND c.mid = m.mid AND m.year < 1980
          and a.aid NOT IN (SELECT c.aid FROM Cast  c
                             WHERE c.mid in (SELECT m.mid 
                                             FROM Movies m 
                                             WHERE m.year >= 1980))
        ORDER BY lname ASC, fname ASC        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q5(self):
        query = '''
        SELECT d.fname, d.lname, count(*) as f
        FROM Movies as m, Movie_Director as md, Directors as d
        WHERE m.mid = md.mid AND md.did = d.did 
        GROUP BY d.lname, d.fname
        ORDER BY f DESC, d.lname, d.fname
        LIMIT 10

        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q6(self):
        query = ''' SELECT m.title, count(DISTINCT c.aid) as castnum
                    FROM Movies as m, Cast as c 
                    WHERE m.mid = c.mid 
                    GROUP BY c.mid
                    ORDER BY castnum DESC 
                    LIMIT 11
        
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q7(self):
        query = ''' SELECT m.title, fm.gencnt, ma.gencnt 
                      from Movies as m,
                           (SELECT c.mid,count(*) gencnt 
                                from cast as c, actors a
                                WHERE c.aid = a.aid and a.gender = 'Female' 
                                GROUP BY c.mid) fm, 
                           (SELECT c.mid,count(*) gencnt 
                                FROM Cast as c, Actors a
                                WHERE a.aid = c.aid AND a.gender = 'Male' 
                                GROUP BY c.mid) ma
                    where m.mid =  fm.mid AND m.mid = ma.mid AND fm.gencnt > ma.gencnt
                    ORDER BY m.title
        

        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q8(self):
        query = ''' SELECT a.fname, a.lname, count(DISTINCT md.did) as cnt
                    FROM Actors as a, Movie_Director as md, Cast as c
                    WHERE a.aid = c.aid AND c.mid = md.mid AND a.aid != md.did 
                    GROUP BY a.fname, a.lname having count(DISTINCT md.did) >= 7
                    ORDER BY cnt DESC

        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q9(self):
        query = ''' SELECT distinct a.fname, a.lname, count(*) as cnt
                    FROM Actors as a, Movies as m,
                        (SELECT distinct a.aid, MIN(m.year) as debut_yr 
                         FROM Actors as a, Movies as m, Cast as c 
                         WHERE a.aid = c.aid AND c.mid = m.mid
                         GROUP BY a.aid) amc
                    WHERE m.year = amc.debut_yr and a.aid  = amc.aid and a.fname like 'D%'
                    GROUP BY a.aid
                    ORDER BY cnt DESC, a.fname, a.lname
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q10(self):
        query = ''' 
        SELECT a.lname, m.title
        FROM Actors as a, Movies as m, Cast as c, Movie_Director as md, Directors as d
        WHERE a.aid = c.aid AND c.mid = m.mid AND m.mid = md.mid AND md.did = d.did AND c.mid = md.mid AND  (a.lname = d.lname)
        ORDER BY a.lname ASC, m.title ASC
        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q11(self):
        query = '''
                DROP VIEW IF EXISTS bacon
                '''
        self.cur.execute(query)

        query = '''
                CREATE VIEW bacon as
                    SELECT DISTINCT a.aid
                    FROM Actors as a, Movies as m, Cast as c
                    WHERE a.aid = c.aid AND c.mid = m.mid AND a.fname = 'Kevin' AND a.lname = 'Bacon'
                '''
        self.cur.execute(query)

        query = '''
                DROP VIEW IF EXISTS baconfilms
                '''
        self.cur.execute(query)

        query = '''
                CREATE VIEW baconfilms as
                    SELECT a.aid 
                    FROM Actors as a, Movies as m, Cast as c, (SELECT DISTINCT m.title as title
                                                  FROM Actors as a, Movies as m, Cast as c
                                                  WHERE a.aid = c.aid AND c.mid = m.mid AND a.fname = 'Kevin' AND a.lname = 'Bacon') as films
                    WHERE films.title = m.title AND a.aid = c.aid AND c.mid = m.mid AND a.aid NOT IN (SELECT * FROM bacon)                          
                '''
        self.cur.execute(query)


        query = '''
                SELECT DISTINCT a.fname, a.lname
                FROM Actors as a, Movies as m, Cast as c, (SELECT DISTINCT m.title as title
                                                           FROM Actors as a, Movies as m, Cast as c, baconfilms as bf
                                                           WHERE a.aid = c.aid AND c.mid = m.mid AND bf.aid = c.aid) as bf1
                WHERE bf1.title = m.title AND a.aid = c.aid AND c.mid = m.mid AND a.aid NOT IN (SELECT * FROM baconfilms) AND a.aid NOT IN (SELECT * FROM bacon)            
                ORDER BY lname, fname
                '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows

    def q12(self):
        query = '''
            DROP VIEW IF EXISTS popularity
        '''
        self.cur.execute(query)

        query = '''
            CREATE VIEW popular as
                SELECT DISTINCT a.aid, avg(m.rank) as avgr, count(m.mid) as movcnt
                FROM Actors as a, Cast as c, Movies as m
                WHERE a.aid = c.aid AND m.mid = c.mid
                GROUP BY a.aid
        '''
        self.cur.execute(query)

        query = '''
            SELECT a.fname, a.lname, p.movcnt, p.avgr
            FROM Actors as a, popular as p
            WHERE a.aid = p.aid 
            GROUP BY a.aid
            ORDER BY p.avgr DESC
            LIMIT 20

        '''
        self.cur.execute(query)
        all_rows = self.cur.fetchall()
        return all_rows


if __name__ == "__main__":
    task = Movie_auto("cs1656-public.db")
    rows = task.q0()
    print(rows)
    print()
    rows = task.q1()
    print(rows)
    print()
    rows = task.q2()
    print(rows)
    print()
    rows = task.q3()
    print(rows)
    print()
    rows = task.q4()
    print(rows)
    print()
    rows = task.q5()
    print(rows)
    print()
    rows = task.q6()
    print(rows)
    print()
    rows = task.q7()
    print(rows)
    print()
    rows = task.q8()
    print(rows)
    print()
    rows = task.q9()
    print(rows)
    print()
    rows = task.q10()
    print(rows)
    print()
    rows = task.q11()
    print(rows)
    print()
    rows = task.q12()
    print(rows)
    print()
