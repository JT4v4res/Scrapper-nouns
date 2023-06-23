from sqlalchemy import create_engine, Column, Integer, String, exc, Table, ForeignKey, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, configure_mappers
import time

engine = create_engine('mysql+pymysql://user:password@localhost:port/Database', echo=True)

Base = declarative_base()

Session = sessionmaker(bind=engine)
session = Session()


class TweetNouns(Base):
    __tablename__ = 'tweet_nouns'
    t_id = Column(Integer, ForeignKey('tweet_text.tweet_id'), primary_key=True, autoincrement=False)
    n_id = Column(Integer, ForeignKey('noun_count.noun_id'), primary_key=True, autoincrement=False)
    tweets = relationship('TweetText', back_populates='association')
    nouns = relationship('NounCount', back_populates='association')

    def __init__(self, t_id, n_id):
        self.t_id = t_id
        self.n_id = n_id


class TweetText(Base):
    __tablename__ = 'tweet_text'
    tweet_id = Column(Integer, primary_key=True)
    content = Column(Text(length=2**32-1))
    association = relationship('TweetNouns', back_populates='tweets')

    def __init__(self, content):
        self.content = content


class NounCount(Base):
    __tablename__ = 'noun_count'
    noun_id = Column(Integer, primary_key=True)
    noun = Column(String(255))
    noun_count = Column(Integer)
    association = relationship('TweetNouns', back_populates='nouns')

    def __init__(self, noun, noun_count):
        self.noun = noun
        self.noun_count = noun_count


configure_mappers()

Base.metadata.create_all(engine)


class SQLEngine:
    def insertManyToMany(self, tweet, noun):
        try:
            twt = self.search(tweet, 1)
            n = self.search(noun, 2)

            if twt and n:
                twtn = TweetNouns(t_id=twt.tweet_id, n_id=n.noun_id)

                session.add(twtn)
                session.commit()

                self.success_msg(f"Insert relationship")
        except exc.SQLAlchemyError as err:
            session.rollback()
            self.fail_msg("Insert relationship", err)
        finally:
            self.close_msg()

    def insertTweet(self, tweet):
        try:
            session.add(tweet)
            session.commit()

            self.success_msg("Insert tweet")
        except exc.SQLAlchemyError as err:
            session.rollback()
            self.fail_msg("Insert tweet", err)
        finally:
            self.close_msg()

    def insertNoun(self, noun):
        try:
            session.add(noun)
            session.commit()

            self.success_msg("Insert noun")
        except exc.SQLAlchemyError as err:
            session.rollback()
            self.fail_msg("Insert noun", err)
        finally:
            self.close_msg()

    def delete(self, value):
        try:
            session.delete(value)
            session.commit()

            self.success_msg("Delete")
        except exc.SQLAlchemyError as err:
            session.rollback()
            self.fail_msg("Delete", err)
        finally:
            self.close_msg()

    def update(self, nounUpdt, newCount):
        try:
            noun = self.search(nounUpdt, 2)

            if noun:
                noun.noun_count = newCount

            session.commit()

            self.success_msg("Update")
        except exc.SQLAlchemyError as err:
            session.rollback()
            self.fail_msg("Update", err)
        finally:
            self.close_msg()

    def search(self, value, opt):
        try:
            query_result = None
            nounCountDict = {}

            if value and opt == 1:
                query_result = session.query(TweetText).filter(TweetText.content == value).first()

                return query_result
            elif value and opt == 2:
                query_result = session.query(NounCount).filter(NounCount.noun == value).first()

                return query_result
            elif opt == 3:
                query_result = session.query(NounCount).all()

                for c in query_result:
                    nounCountDict[c.noun.lower().strip()] = c.noun_count

                if nounCountDict:
                    self.success_msg("Select")
                    return nounCountDict
                else: 
                    self.success_msg("Select")
                    return {}

            elif opt == 4:
                tweetNoun = session.query(TweetNouns).filter(TweetNouns.t_id == value[0], TweetNouns.n_id == value[1]).first()

                return tweetNoun
            
            elif opt == 5:
                tweetNoun = session.query(TweetNouns).filter(TweetNouns.t_id == value).first()

                return tweetNoun

            self.success_msg("Select")
        except exc.SQLAlchemyError as err:
            self.fail_msg("Select", err)
        finally:
            self.close_msg()

    @staticmethod
    def success_msg(value):
        print("----------------------------")
        print(f"{value} completed successfully <{ time.strftime('%H:%M:%S', time.localtime(time.time())) }>")

    @staticmethod
    def fail_msg(value, err):
        print("----------------------------")
        print("Failed to {} in table: {}, {}".format(value, err, time.strftime('%H:%M:%S', time.localtime(time.time()))))

    @staticmethod
    def close_msg():
        print("----------------------------")
        print(f"Closed connection <{ time.strftime('%H:%M:%S', time.localtime(time.time())) }>")
        print("----------------------------")
