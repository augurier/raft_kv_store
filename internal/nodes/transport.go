package nodes

type ClientInterface interface{
    Close() error
}

type Transport interface {
    DialHTTPWithTimeout(network string, myId string, peerId string) (ClientInterface, error)
    CallWithTimeout(client ClientInterface, serviceMethod string, args interface{}, reply interface{}) error
}
