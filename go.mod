module github.com/SlothNinja/rating

go 1.14

require (
	cloud.google.com/go v0.74.0
	cloud.google.com/go/datastore v1.3.0
	github.com/SlothNinja/contest v1.0.0
	github.com/SlothNinja/glicko v1.0.0
	github.com/SlothNinja/log v0.0.2
	github.com/SlothNinja/restful v1.0.0
	github.com/SlothNinja/sn v1.0.1
	github.com/SlothNinja/type v1.0.1
	github.com/SlothNinja/user v1.0.10
	github.com/gin-gonic/gin v1.6.3
	google.golang.org/api v0.36.0
	google.golang.org/genproto v0.0.0-20201214200347-8c77b98c765d
)

replace github.com/SlothNinja/user => ../user
