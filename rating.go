package rating

import (
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/SlothNinja/contest"
	"github.com/SlothNinja/glicko"
	"github.com/SlothNinja/log"
	"github.com/SlothNinja/restful"
	gtype "github.com/SlothNinja/type"
	"github.com/SlothNinja/user"
	"github.com/gin-gonic/gin"
	"google.golang.org/api/iterator"
	"google.golang.org/appengine"
	"google.golang.org/appengine/taskqueue"
)

const (
	currentRatingsKey = "CurrentRatings"
	projectedKey      = "Projected"
	homePath          = "/"
)

func CurrentRatingsFrom(c *gin.Context) (rs CurrentRatings) {
	rs, _ = c.Value(currentRatingsKey).(CurrentRatings)
	return
}

func ProjectedFrom(c *gin.Context) (r *Rating) {
	r, _ = c.Value(projectedKey).(*Rating)
	return
}

func AddRoutes(prefix string, engine *gin.Engine) {
	g1 := engine.Group(prefix + "s")
	g1.POST("/userUpdate", updateUser)

	g1.GET("/update/:type", Update)

	g1.GET("/show/:type", Index)

	g1.POST("/show/:type/json", JSONFilteredAction)
}

// Ratings
type Ratings []*Rating
type Rating struct {
	// ID     int64          `gae:"$id"`
	// Parent *datastore.Key `gae:"$parent"`
	Key *datastore.Key `datastore:"__key__"`
	Common
}

type CurrentRatings []*CurrentRating
type CurrentRating struct {
	// ID     string         `gae:"$id"`
	// Parent *datastore.Key `gae:"$parent"`
	Key *datastore.Key `datastore:"__key__"`
	Common
}

type Common struct {
	generated bool
	Type      gtype.Type
	R         float64
	RD        float64
	Low       float64
	High      float64
	Leader    bool
	CreatedAt time.Time
	UpdatedAt time.Time
}

func (r *CurrentRating) Rank() *glicko.Rank {
	return &glicko.Rank{
		R:  r.R,
		RD: r.RD,
	}
}

func New(c *gin.Context, id int64, pk *datastore.Key, t gtype.Type, params ...float64) *Rating {
	r, rd := defaultR, defaultRD
	if len(params) == 2 {
		r, rd = params[0], params[1]
	}

	rating := new(Rating)
	rating.Key = datastore.IDKey(rKind, id, pk)
	rating.R = r
	rating.RD = rd
	rating.Low = r - (2.0 * rd)
	rating.High = r + (2.0 * rd)
	rating.Type = t
	return rating
}

func NewCurrent(c *gin.Context, pk *datastore.Key, t gtype.Type, params ...float64) *CurrentRating {
	r, rd := defaultR, defaultRD
	if len(params) == 2 {
		r, rd = params[0], params[1]
	}

	rating := new(CurrentRating)
	rating.Key = datastore.NameKey(crKind, t.SString(), pk)
	// rating.ID = t.SString()
	// rating.Parent = pk
	rating.R = r
	rating.RD = rd
	rating.Low = r - (2.0 * rd)
	rating.High = r + (2.0 * rd)
	rating.Type = t
	return rating
}

const (
	defaultR  float64 = 1500
	defaultRD float64 = 350
)

const (
	rKind  = "Rating"
	crKind = "CurrentRating"
)

func singleError(e error) error {
	if e == nil {
		return e
	}
	if me, ok := e.(datastore.MultiError); ok {
		return me[0]
	}
	return e
}

// Get Current Rating for gtype.Type and user associated with uKey
func Get(c *gin.Context, uKey *datastore.Key, t gtype.Type) (*CurrentRating, error) {
	ratings, err := GetMulti(c, []*datastore.Key{uKey}, t)
	return ratings[0], singleError(err)
}

func GetMulti(c *gin.Context, uKeys []*datastore.Key, t gtype.Type) (CurrentRatings, error) {
	dsClient, err := datastore.NewClient(c, "")
	if err != nil {
		return nil, err
	}

	l := len(uKeys)
	ratings := make(CurrentRatings, l)
	ks := make([]*datastore.Key, l)
	for i, uKey := range uKeys {
		ratings[i] = NewCurrent(c, uKey, t)
		ks[i] = ratings[i].Key
	}

	err = dsClient.GetMulti(c, ks, ratings)
	if err == nil {
		return ratings, nil
	}

	me := err.(datastore.MultiError)
	isNil := true
	for i := range uKeys {
		if me[i] == datastore.ErrNoSuchEntity {
			ratings[i].generated = true
			me[i] = nil
		} else {
			isNil = false
		}
	}
	if isNil {
		return ratings, nil
	} else {
		return ratings, me
	}
}

func GetAll(c *gin.Context, uKey *datastore.Key) (CurrentRatings, error) {
	dsClient, err := datastore.NewClient(c, "")
	if err != nil {
		return nil, err
	}

	l := len(gtype.Types)
	rs := make(CurrentRatings, l)
	ks := make([]*datastore.Key, l)
	for i, t := range gtype.Types {
		rs[i] = NewCurrent(c, uKey, t)
		ks[i] = rs[i].Key
		// rs[i].Parent = uKey
		// rs[i].ID = t.SString()
	}

	err = dsClient.GetMulti(c, ks, rs)
	if err == nil {
		return nil, err
	}

	merr, ok := err.(datastore.MultiError)
	if !ok {
		return nil, err
	}

	enil := true
	for i, e := range merr {
		if e == datastore.ErrNoSuchEntity {
			rs[i].generated = true
			merr[i] = nil
		} else if e != nil {
			enil = false
		}
	}

	if enil {
		return rs, nil
	}
	return nil, merr
}

// func GetHistory(c *gin.Context, uKey *datastore.Key, t gtype.Type) (Ratings, error) {
// 	ratings := make(Ratings, len(gtype.Types))
// 	keys := make([]*datastore.Key, len(gtype.Types))
// 	for i, t := range gtype.Types {
// 		ratings[i] = New(c, uKey, t)
// 		if ok := datastore.PopulateKey(ratings[i], keys[i]); !ok {
// 			return nil, fmt.Errorf("Unable to populate rating with key.")
// 		}
// 	}
// 	if err := datastore.Get(c, ratings); err != nil {
// 		return nil, err
// 	}
// 	return ratings, nil
// }

func GetFor(c *gin.Context, t gtype.Type) (CurrentRatings, error) {
	dsClient, err := datastore.NewClient(c, "")
	if err != nil {
		return nil, err
	}

	q := datastore.NewQuery(crKind).
		Ancestor(user.RootKey(c)).
		Filter("Type=", int(t)).
		Order("-Low")

	var rs CurrentRatings
	_, err = dsClient.GetAll(c, q, &rs)
	if err != nil {
		return nil, err
	}
	return rs, nil
}

func (rs CurrentRatings) Projected(c *gin.Context, cm contest.ContestMap) (CurrentRatings, error) {
	ratings := make(CurrentRatings, len(rs))
	for i, r := range rs {
		var err error
		if ratings[i], err = r.Projected(c, cm[r.Type]); err != nil {
			return nil, err
		}
	}
	return ratings, nil
}

func (r *CurrentRating) Projected(c *gin.Context, cs contest.Contests) (*CurrentRating, error) {
	log.Debugf("Entering r.Projected")
	defer log.Debugf("Exiting r.Projected")

	l := len(cs)
	if l == 0 && r.generated {
		log.Debugf("rating: %#v generated: %v", r, r.generated)
		return r, nil
	}

	gcs := make(glicko.Contests, l)
	for i, c := range cs {
		gcs[i] = glicko.NewContest(c.Outcome, c.R, c.RD)
	}

	rating, err := glicko.UpdateRating(r.Rank(), gcs)
	if err != nil {
		return nil, err
	}

	return NewCurrent(c, r.Key.Parent, r.Type, rating.R, rating.RD), nil
}

func (r *CurrentRating) Generated() bool {
	return r.generated
}

func Index(c *gin.Context) {
	log.Debugf("Entering")
	defer log.Debugf("Exiting")

	t := gtype.ToType[c.Param("type")]
	c.HTML(http.StatusOK, "rating/index", gin.H{
		"Type":      t,
		"Heading":   "Ratings: " + t.String(),
		"Types":     gtype.Types,
		"Context":   c,
		"VersionID": appengine.VersionID(c),
		"CUser":     user.CurrentFrom(c),
	})
}

func getAllQuery(c *gin.Context) *datastore.Query {
	return datastore.NewQuery(crKind).Ancestor(user.RootKey(c))
}

func getFiltered(c *gin.Context, t gtype.Type, leader bool, offset, limit int32) (CurrentRatings, int64, error) {
	dsClient, err := datastore.NewClient(c, "")
	if err != nil {
		return nil, 0, err
	}

	q := getAllQuery(c)

	if leader {
		q = q.Filter("Leader=", true)
	}

	if t != gtype.NoType {
		q = q.Filter("Type=", int(t))
	}

	var cnt int64
	count, err := dsClient.Count(c, q)
	if err != nil {
		return nil, 0, err
	}
	cnt = int64(count)

	q = q.Offset(int(offset)).
		Limit(int(limit)).
		Order("-Low")

	var rs CurrentRatings
	_, err = dsClient.GetAll(c, q, &rs)
	if err != nil {
		return nil, 0, err
	}

	return rs, cnt, err
}

func (rs CurrentRatings) getUsers(c *gin.Context) (user.Users, error) {
	log.Debugf("Entering")
	defer log.Debugf("Exiting")

	dsClient, err := datastore.NewClient(c, "")
	if err != nil {
		return nil, err
	}

	l := len(rs)
	us := make(user.Users, l)
	ks := make([]*datastore.Key, l)
	for i := range rs {
		//log.Debugf("rs[i]: %#v", rs[i])
		us[i] = user.New(c, 0)
		// if ok := datastore.PopulateKey(us[i], rs[i].Parent); !ok {
		// 	log.Debugf("Unable to populate user with key: %v", rs[i].Parent)
		// 	return nil, fmt.Errorf("Unable to populate user with key.")
		// }
	}

	err = dsClient.GetMulti(c, ks, us)
	if err != nil {
		return nil, err
	}
	return us, nil
}

func (rs CurrentRatings) getProjected(c *gin.Context) (ps CurrentRatings, err error) {
	log.Debugf("Entering")
	defer log.Debugf("Exiting")

	ps = make(CurrentRatings, len(rs))
	var cs contest.Contests
	for i, r := range rs {
		uKey := r.Key.Parent

		if cs, err = contest.UnappliedFor(c, uKey, r.Type); err != nil {
			return
		}

		if ps[i], err = r.Projected(c, cs); err != nil {
			return
		}

		if r.generated && r.generated && len(cs) == 0 {
			ps[i].generated = true
		}
	}
	return
}

func For(c *gin.Context, u *user.User, t gtype.Type) (*CurrentRating, error) {
	return Get(c, u.Key, t)
	// return Get(c, datastore.KeyForObj(c, u), t)
}

func MultiFor(c *gin.Context, u *user.User) (CurrentRatings, error) {
	return GetAll(c, u.Key)
	// return GetAll(c, datastore.KeyForObj(c, u))
}

// AddMulti has a limit of 100 tasks.  Thus, the batching.
func Update(c *gin.Context) {
	log.Debugf("Entering")
	defer log.Debugf("Exiting")

	dsClient, err := datastore.NewClient(c, "")
	if err != nil {
		log.Errorf(err.Error())
		c.AbortWithStatus(http.StatusInternalServerError)
	}

	var tk *taskqueue.Task

	tp := c.Param("type")
	q := user.AllQuery(c).
		KeysOnly()
	path := "/ratings/userUpdate"
	ts := make([]*taskqueue.Task, 0, 100)
	o := taskqueue.RetryOptions{RetryLimit: 5}

	tparams := make(url.Values)
	tparams.Set("type", tp)

	it := dsClient.Run(c, q)
	for {
		k, err := it.Next(nil)
		if err == iterator.Done {
			break
		}

		if err != nil {
			log.Errorf(err.Error())
			c.AbortWithStatus(http.StatusInternalServerError)
		}

		log.Debugf("k: %s", k)
		tparams.Set("uid", fmt.Sprintf("%v", k.ID))
		tk = taskqueue.NewPOSTTask(path, tparams)
		tk.RetryOptions = &o
		ts = append(ts, tk)
	}

	_, err = taskqueue.AddMulti(c, ts, "")
	if err != nil {
		log.Errorf(err.Error())
		c.AbortWithStatus(http.StatusInternalServerError)
	}
}

func updateUser(c *gin.Context) {
	log.Debugf("Entering")
	defer log.Debugf("Exiting")

	dsClient, err := datastore.NewClient(c, "")
	if err != nil {
		log.Errorf(err.Error())
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	uid, err := strconv.ParseInt(c.PostForm("uid"), 10, 64)
	if err != nil {
		log.Errorf("Invalid uid: %s received", c.PostForm("uid"))
		c.AbortWithStatus(http.StatusNotFound)
		return
	}

	u := user.New(c, uid)
	err = dsClient.Get(c, u.Key, u)
	if err != nil {
		log.Errorf("Unable to find user for id: %s", c.PostForm("uid"))
		c.AbortWithStatus(http.StatusNotFound)
		return
	}

	t := gtype.ToType[c.PostForm("type")]

	r, err := For(c, u, t)
	if err != nil {
		log.Errorf("Unable to find rating for userid: %s", c.PostForm("uid"))
		c.AbortWithStatus(http.StatusNotFound)
		return
	}

	cs, err := contest.UnappliedFor(c, u.Key, t)
	if err != nil {
		log.Errorf("Ratings update error when getting unapplied contests for user ID: %v.\n Error: %s", u.ID, err)
		c.AbortWithStatus(http.StatusInternalServerError)
		return

	}

	p, err := r.Projected(c, cs)
	if err != nil {
		log.Errorf("Ratings update error when getting projected rating for user ID: %v\n Error: %s", u.ID, err)
		c.AbortWithStatus(http.StatusInternalServerError)
		return
	}

	if time.Since(time.Time(r.UpdatedAt)) < 504*time.Hour {
		log.Debugf("Did not update rating for user ID: %v", u.ID)
		log.Debugf("Rating updated %s ago.", time.Since(time.Time(r.UpdatedAt)))
		return
	}

	log.Debugf("Processing rating update for user ID: %v", u.ID)
	log.Debugf("Projected rating: %#v", p)
	log.Debugf("Unapplied contest count: %v", len(cs))

	log.Debugf("Reached for that ranges projected")
	const threshold float64 = 200.0
	// Update leader value to indicate whether present on leader boards
	p.Leader = p.RD < threshold

	var (
		es []interface{}
		ks []*datastore.Key
	)

	if !p.Generated() {
		r := New(c, 0, p.Key.Parent, p.Type, p.R, p.RD)
		es = append(es, p, r)
		ks = append(ks, p.Key, r.Key)

	}

	log.Debugf("Reached for that unapplied contests")
	for _, c := range cs {
		c.Applied = true
		es = append(es, c)
		ks = append(ks, c.Key)
	}

	_, err = dsClient.RunInTransaction(c, func(tx *datastore.Transaction) error {
		_, err := tx.PutMulti(ks, es)
		return err
	})
	if err != nil {
		log.Errorf("Ratings update err when saving updated ratings for user ID: %v\n Error: %s", u.ID, err)
		c.AbortWithStatus(http.StatusInternalServerError)
	}
	log.Debugf("Reached RunInTransaction")
}

func Fetch(c *gin.Context) {
	if CurrentRatingsFrom(c) != nil {
		return
	}

	u := user.Fetched(c)
	if u == nil {
		restful.AddErrorf(c, "Unable to get ratings.")
		c.Redirect(http.StatusSeeOther, homePath)
		return
	}

	if rs, err := MultiFor(c, u); err != nil {
		restful.AddErrorf(c, err.Error())
		c.Redirect(http.StatusSeeOther, homePath)
	} else {
		c.Set(currentRatingsKey, rs)
	}
}

func Fetched(c *gin.Context) CurrentRatings {
	return CurrentRatingsFrom(c)
}

func FetchProjected(c *gin.Context) {
	if ProjectedFrom(c) != nil {
		return
	}

	rs := Fetched(c)
	if rs == nil {
		restful.AddErrorf(c, "Unable to get projected ratings")
		c.Redirect(http.StatusSeeOther, homePath)
		return
	}

	cm, err := contest.Unapplied(c, user.Fetched(c).Key)
	if err != nil {
		restful.AddErrorf(c, err.Error())
		c.Redirect(http.StatusSeeOther, homePath)
		return
	}

	if pr, err := rs.Projected(c, cm); err != nil {
		restful.AddErrorf(c, err.Error())
		c.Redirect(http.StatusSeeOther, homePath)
	} else {
		c.Set(projectedKey, pr)
	}
}

func Projected(c *gin.Context) (pr Ratings) {
	pr, _ = c.Value("Projected").(Ratings)
	return
}

//func (r *Rating) Save(ch chan<- datastore.Property) error {
//	// Time stamp
//	t := time.Now()
//	if r.CreatedAt.IsZero() {
//		r.CreatedAt = t
//	}
//	r.UpdatedAt = t
//	return datastore.SaveStruct(r, ch)
//}
//
//func (r *Rating) Load(ch <-chan datastore.Property) error {
//	return datastore.LoadStruct(r, ch)
//}

type jRating struct {
	Type template.HTML `json:"type"`
	R    float64       `json:"r"`
	RD   float64       `json:"rd"`
	Low  float64       `json:"low"`
	High float64       `json:"high"`
}

type jCombined struct {
	Rank      int           `json:"rank"`
	Gravatar  template.HTML `json:"gravatar"`
	Name      template.HTML `json:"name"`
	Type      template.HTML `json:"type"`
	Current   template.HTML `json:"current"`
	Projected template.HTML `json:"projected"`
}

func JSONIndexAction(c *gin.Context) {
	log.Debugf("Entering")
	defer log.Debugf("Exiting")

	dsClient, err := datastore.NewClient(c, "")
	if err != nil {
		log.Errorf(err.Error())
		c.Redirect(http.StatusSeeOther, homePath)
		return
	}

	uid, err := strconv.ParseInt(c.Param("uid"), 10, 64)
	if err != nil {
		log.Errorf("rating#JSONIndexAction BySID Error: %s", err)
		c.Redirect(http.StatusSeeOther, homePath)
		return
	}

	u := user.New(c, uid)
	err = dsClient.Get(c, u.Key, u)
	if err != nil {
		log.Errorf("rating#JSONIndexAction unable to find user for uid: %d", uid)
		c.Redirect(http.StatusSeeOther, homePath)
		return
	}

	rs, err := MultiFor(c, u)
	if err != nil {
		log.Errorf("rating#JSONIndexAction MultiFor Error: %s", err)
		c.Redirect(http.StatusSeeOther, homePath)
		return
	}

	ps, err := rs.getProjected(c)
	if err != nil {
		log.Errorf("rating#getProjected Error: %s", err)
		c.Redirect(http.StatusSeeOther, homePath)
		return
	}

	if data, err := singleUser(c, u, rs, ps); err != nil {
		c.JSON(http.StatusOK, fmt.Sprintf("%v", err))
	} else {
		c.JSON(http.StatusOK, data)
	}
}

func JSONFilteredAction(c *gin.Context) {
	log.Debugf("Entering")
	defer log.Debugf("Exiting")

	t := gtype.Get(c)

	var offset, limit int32 = 0, -1
	if o, err := strconv.ParseInt(c.PostForm("start"), 10, 64); err == nil && o >= 0 {
		offset = int32(o)
	}

	if l, err := strconv.ParseInt(c.PostForm("length"), 10, 64); err == nil {
		limit = int32(l)
	}

	rs, cnt, err := getFiltered(c, t, true, offset, limit)
	if err != nil {
		log.Errorf("rating#getFiltered Error: %s", err)
		return
	}
	log.Debugf("rs: %#v", rs)

	us, err := rs.getUsers(c)
	if err != nil {
		log.Errorf("rating#getUsers Error: %s", err)
		return
	}
	log.Debugf("us: %#v", us)

	ps, err := rs.getProjected(c)
	if err != nil {
		log.Errorf("rating#getProjected Error: %s", err)
		return
	}
	log.Debugf("ps: %#v", ps)

	if data, err := toCombined(c, us, rs, ps, offset, cnt); err != nil {
		log.Debugf("toCombined error: %v", err)
		c.JSON(http.StatusOK, fmt.Sprintf("%v", err))
	} else {
		c.JSON(http.StatusOK, data)
	}
}

type jCombinedRatingsIndex struct {
	Data            []*jCombined `json:"data"`
	Draw            int          `json:"draw"`
	RecordsTotal    int          `json:"recordsTotal"`
	RecordsFiltered int          `json:"recordsFiltered"`
}

func (r *CurrentRating) String() string {
	return fmt.Sprintf("%.f (%.f : %.f)", r.Low, r.R, r.RD)
}

func singleUser(c *gin.Context, u *user.User, rs, ps CurrentRatings) (table *jCombinedRatingsIndex, err error) {
	log.Debugf("Entering singleUser")
	defer log.Debugf("Exiting singleUser")

	table = new(jCombinedRatingsIndex)
	l1, l2 := len(rs), len(ps)
	if l1 != l2 {
		err = fmt.Errorf("Length mismatch between ratings and projected ratings l1: %d l2: %d.", l1, l2)
		return
	}

	table.Data = make([]*jCombined, 0)
	for i, r := range rs {
		if p := ps[i]; !p.generated {
			table.Data = append(table.Data, &jCombined{
				Gravatar:  user.Gravatar(u),
				Name:      u.Link(),
				Type:      template.HTML(r.Type.String()),
				Current:   template.HTML(r.String()),
				Projected: template.HTML(p.String()),
			})
		}
	}

	var draw int
	if draw, err = strconv.Atoi(c.PostForm("draw")); err != nil {
		log.Debugf("strconv.Atoi error: %v", err)
		return
	}

	table.Draw = draw
	table.RecordsTotal = l1
	table.RecordsFiltered = l2
	return
}
func toCombined(c *gin.Context, us user.Users, rs, ps CurrentRatings, o int32, cnt int64) (*jCombinedRatingsIndex, error) {
	table := new(jCombinedRatingsIndex)
	l1, l2 := len(rs), len(ps)
	if l1 != l2 {
		return nil, fmt.Errorf("Length mismatch between ratings and projected ratings l1: %d l2: %d.", l1, l2)
	}
	table.Data = make([]*jCombined, 0)
	for i, r := range rs {
		if !r.generated {
			table.Data = append(table.Data, &jCombined{
				Rank:      i + int(o) + 1,
				Gravatar:  user.Gravatar(us[i]),
				Name:      us[i].Link(),
				Type:      template.HTML(r.Type.String()),
				Current:   template.HTML(r.String()),
				Projected: template.HTML(ps[i].String()),
			})
		}
	}

	if draw, err := strconv.Atoi(c.PostForm("draw")); err != nil {
		return nil, err
	} else {
		table.Draw = draw
	}

	table.RecordsTotal = int(cnt)
	table.RecordsFiltered = int(cnt)
	return table, nil
}

func IncreaseFor(c *gin.Context, u *user.User, t gtype.Type, cs contest.Contests) (cr, nr *CurrentRating, err error) {
	log.Debugf("Entering")
	defer log.Debugf("Exiting")

	// k := datastore.KeyForObj(c, u)
	k := u.Key

	var ucs contest.Contests
	if ucs, err = contest.UnappliedFor(c, k, t); err != nil {
		return
	}

	var r *CurrentRating
	if r, err = For(c, u, t); err != nil {
		return
	}

	if cr, err = r.Projected(c, ucs); err != nil {
		return
	}

	nr, err = r.Projected(c, append(ucs, filterContestsFor(cs, k)...))
	return
}

func filterContestsFor(cs contest.Contests, pk *datastore.Key) (fcs contest.Contests) {
	for _, c := range cs {
		if c.Key.Parent.Equal(pk) {
			fcs = append(fcs, c)
		}
	}
	return
}
