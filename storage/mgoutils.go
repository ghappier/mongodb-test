package storage

import (
	"fmt"
	"sync"
	"time"

	"github.com/ghappier/mongodb-test/config"
	"gopkg.in/mgo.v2"
)

var (
	mgoUtils *MgoUtils
	onceMgo  sync.Once
)

type MgoUtils struct {
	lock       *sync.RWMutex
	sessionMap map[string]SessionBean
}

type SessionBean struct {
	mongodbConfig *config.MongodbConfig
	mgoSession    *mgo.Session
}

func GetMgoUtilsInstance() *MgoUtils {
	onceMgo.Do(func() {
		mLock := new(sync.RWMutex)
		mSessionMap := make(map[string]SessionBean)
		mgoUtils = &MgoUtils{
			lock:       mLock,
			sessionMap: mSessionMap,
		}
		go func() {
			timer := time.NewTicker(1 * time.Second)
			for {
				select {
				case <-timer.C:
					for _, sessionBean := range mgoUtils.sessionMap {
						if sessionBean.mgoSession != nil {
							err := sessionBean.mgoSession.Ping()
							if err != nil {
								fmt.Println("Lost connection to db!")
								sessionBean.mgoSession.Refresh()
								err = sessionBean.mgoSession.Ping()
								if err == nil {
									fmt.Println("Reconnect to db successful.")
								}
							}
						}
					}
				}
			}
		}()
	})
	return mgoUtils
}

func (m *MgoUtils) GetSession(mongodbConfig *config.MongodbConfig) *mgo.Session {
	var sessionBean SessionBean
	var present bool
	m.lock.RLock()
	if sessionBean, present = m.sessionMap[mongodbConfig.Url]; !present {
		m.lock.RUnlock()
		m.lock.Lock()
		if sessionBean, present = m.sessionMap[mongodbConfig.Url]; !present {
			mSession, err := Mydial(mongodbConfig)
			mSession.SetPoolLimit(mongodbConfig.PoolLimit)
			if err == nil {
				sessionBean = SessionBean{
					mongodbConfig: mongodbConfig,
					mgoSession:    mSession,
				}
				m.sessionMap[mongodbConfig.Url] = sessionBean
			}
		}
		m.lock.Unlock()
	} else {
		m.lock.RUnlock()
	}
	if sessionBean.mgoSession != nil {
		return sessionBean.mgoSession.Clone()
	}
	return nil
}

//连接mongodb
func Mydial(mongodbConfig *config.MongodbConfig) (*mgo.Session, error) {
	var url string = fmt.Sprintf("mongodb://%s/%s?maxPoolSize=%d", mongodbConfig.Url, mongodbConfig.Db, mongodbConfig.PoolLimit)
	//var url string = fmt.Sprintf("mongodb://%s/%s", mongodbConfig.Url, mongodbConfig.Db)
	session, err := mgo.Dial(url)
	if err == nil {
		session.SetSyncTimeout(1 * time.Minute)
		session.SetSocketTimeout(1 * time.Minute)
	}
	return session, err
}
