import math
import numpy
import heapq


# a data container for all pertinent information related to fares. (Should we
# add an underway flag and require taxis to acknowledge collection to the dispatcher?)
class FareEntry:

    def __init__(self, origin, dest, time, price=0, taxiIndex=-1):
        self.origin = origin
        self.destination = dest
        self.calltime = time
        self.price = price
        # the taxi allocated to service this fare. -1 if none has been allocated
        self.taxi = taxiIndex
        # a list of indices of taxis that have bid on the fare.
        self.bidders = []


'''
A Dispatcher is a static agent whose job is to allocate fares amongst available taxis. Like the taxis, all
the relevant functionality happens in ClockTick. The Dispatcher has a list of taxis, a map of the service area,
and a dictionary of active fares (ones which have called for a ride) that it can use to manage the allocations.
Taxis bid after receiving the price, which should be decided by the Dispatcher, and once a 'satisfactory' number
of bids are in, the dispatcher should run allocateFare in its world (parent) to inform the winning bidder that they
now have the fare.
'''


class Dispatcher:

    # constructor only needs to know the world it lives in, although you can also populate its knowledge base
    # with taxi and map information.
    def __init__(self, parent, taxis=None, serviceMap=None):

        self._parent = parent
        # our incoming account
        self._revenue = 0
        # the list of taxis
        self._taxis = taxis
        if self._taxis is None:
            self._taxis = []
        # fareBoard will be a nested dictionary indexed by origin, then destination, then call time.
        # Its values are FareEntries. The nesting structure provides for reasonably fast lookup; it's
        # more or less a multi-level hash.
        self._fareBoard = {}
        # serviceMap gives the dispatcher its service area
        self._map = serviceMap
        self.fareAmountConstraint = {}

    # _________________________________________________________________________________________________________
    # methods to add objects to the Dispatcher's knowledge base

    # make a new taxi known.
    def addTaxi(self, taxi):
        if taxi not in self._taxis:
            self._taxis.append(taxi)

    # incrementally add to the map. This can be useful if, e.g. the world itself has a set of
    # nodes incrementally added. It can then call this function on the dispatcher to add to
    # its map
    def addMapNode(self, coords, neighbours):
        if self._parent is None:
            return AttributeError("This Dispatcher does not exist in any world")
        node = self._parent.getNode(coords[0], coords[1])
        if node is None:
            return KeyError("No such node: {0} in this Dispatcher's service area".format(coords))
        # build up the neighbour dictionary incrementally so we can check for invalid nodes.
        neighbourDict = {}
        for neighbour in neighbours:
            neighbourCoords = (neighbour[1], neighbour[2])
            neighbourNode = self._parent.getNode(neighbour[1], neighbour[2])
            if neighbourNode is None:
                return KeyError(
                    "Node {0} expects neighbour {1} which is not in this Dispatcher's service area".format(coords,
                                                                                                           neighbour))
            neighbourDict[neighbourCoords] = (neighbour[0], self._parent.distance2Node(node, neighbourNode))
        self._map[coords] = neighbourDict

    # importMap gets the service area map, and can be brought in incrementally as well as
    # in one wodge.
    def importMap(self, newMap):
        # a fresh map can just be inserted
        if self._map is None:
            self._map = newMap
        # but importing a new map where one exists implies adding to the
        # existing one. (Check that this puts in the right values!)
        else:
            for node in newMap.items():
                neighbours = [(neighbour[1][0], neighbour[0][0], neighbour[0][1]) for neighbour in node[1].items()]
                self.addMapNode(node[0], neighbours)

    # any legacy fares or taxis from a previous dispatcher can be imported here - future functionality,
    # for the most part
    def handover(self, parent, origin, destination, time, taxi, price):
        if self._parent == parent:
            # handover implies taxis definitely known to a previous dispatcher. The current
            # dispatcher should thus be made aware of them
            if taxi not in self._taxis:
                self._taxis.append(taxi)
            # add any fares found along with their allocations
            self.newFare(parent, origin, destination, time)
            self._fareBoard[origin][destination][time].taxi = self._taxis.index(taxi)
            self._fareBoard[origin][destination][time].price = price

    # --------------------------------------------------------------------------------------------------------------
    # runtime methods used to inform the Dispatcher of real-time events

    # fares will call this when they appear to signal a request for service.
    def newFare(self, parent, origin, destination, time):
        # only add new fares coming from the same world
        if parent == self._parent:
            fare = FareEntry(origin, destination, time)
            if origin in self._fareBoard:
                if destination not in self._fareBoard[origin]:
                    self._fareBoard[origin][destination] = {}
            else:
                self._fareBoard[origin] = {destination: {}}
            # overwrites any existing fare with the same (origin, destination, calltime) triplet, but
            # this would be equivalent to saying it was the same fare, at least in this world where
            # a given Node only has one fare at a time.
            self._fareBoard[origin][destination][time] = fare

    # abandoning fares will call this to cancel their request
    def cancelFare(self, parent, origin, destination, calltime):
        # if the fare exists in our world,
        if parent == self._parent and origin in self._fareBoard:
            if destination in self._fareBoard[origin]:
                if calltime in self._fareBoard[origin][destination]:
                    # get rid of it
                    print("Fare ({0},{1}) cancelled".format(origin[0], origin[1]))
                    # inform taxis that the fare abandoned
                    self._parent.cancelFare(origin, self._taxis[self._fareBoard[origin][destination][calltime].taxi])
                    del self._fareBoard[origin][destination][calltime]
                if len(self._fareBoard[origin][destination]) == 0:
                    del self._fareBoard[origin][destination]
                if len(self._fareBoard[origin]) == 0:
                    del self._fareBoard[origin]

    # taxis register their bids for a fare using this mechanism
    def fareBid(self, origin, taxi):
        # rogue taxis (not known to the dispatcher) can't bid on fares
        if taxi in self._taxis:
            # everyone else bids on fares available
            if origin in self._fareBoard:
                for destination in self._fareBoard[origin].keys():
                    for time in self._fareBoard[origin][destination].keys():
                        # as long as they haven't already been allocated
                        if self._fareBoard[origin][destination][time].taxi == -1:
                            self._fareBoard[origin][destination][time].bidders.append(self._taxis.index(taxi))
                            # only one fare per origin can be actively open for bid, so
                            # immediately return once we[ve found it
                            return

    # fares call this (through the parent world) when they have reached their destination
    def recvPayment(self, parent, amount):
        # don't take payments from dodgy alternative universes
        if self._parent == parent:
            self._revenue += amount

    # ________________________________________________________________________________________________________________

    # clockTick is called by the world and drives the simulation for the Dispatcher. It must, at minimum, handle the
    # 2 main functions the dispatcher needs to run in the world: broadcastFare(origin, destination, price) and
    # allocateFare(origin, taxi).
    def clockTick(self, parent):
        self.displayRevenues()
        if self._parent == parent:
            for origin in self._fareBoard.keys():
                for destination in self._fareBoard[origin].keys():
                    # TODO - if you can come up with something better. Not essential though.
                    # not super-efficient here: need times in order, dictionary view objects are not
                    # sortable because they are an iterator, so we need to turn the times into a
                    # sorted list. Hopefully fareBoard will never be too big
                    for time in sorted(list(self._fareBoard[origin][destination].keys())):
                        if self._fareBoard[origin][destination][time].price == 0:
                            self._fareBoard[origin][destination][time].price = self._costFare(
                                self._fareBoard[origin][destination][time])
                            # broadcastFare actually returns the number of taxis that got the info, if you
                            # wish to use that information in the decision over when to allocate
                            self._parent.broadcastFare(origin,
                                                       destination,
                                                       self._fareBoard[origin][destination][time].price)
                        elif self._fareBoard[origin][destination][time].taxi < 0 and len(
                                self._fareBoard[origin][destination][time].bidders) > 0:
                            self._allocateFare(origin, destination, time)

    # ----------------------------------------------------------------------------------------------------------------
    def _costFare(self, fare):
        # Since travel time already puts traffic into consideration, when I create cost samples they are
        # automatically include traffic prices.
        timeToDestination = self._parent.travelTime(self._parent.getNode(fare.origin[0], fare.origin[1]),
                                                    self._parent.getNode(fare.destination[0], fare.destination[1]))
        # This variable indicates the maximum allowed cost before the fare is abandoned
        maximumCostAllowed = 10 * timeToDestination

        # This function creates cost samples all the way to 200 (if it gets there). Checks the maximum value
        # against the maximum allowed and chooses the highest cost possible for maximum revenue
        costSample = None
        if timeToDestination > 0:
            for i in range(200):
                # If each iteration is less than the maximum allowed, adds it to the cost samples
                if (i + timeToDestination / 0.9) < maximumCostAllowed - 1:
                    costSample = i + timeToDestination / 0.9
                else:
                    # If it's above the maximum allowed, it adds 1 by one until it reaches the maximum.
                    # This ensures the cost is as optimized as possible.
                    # This else is not extremely needed because it usually only runs once since it's already
                    # close to the maximum, but I wanted to optimize it.
                    while costSample < maximumCostAllowed - 1:
                        costSample += 1
                    break
        # If cost samples isn't empty(timeToDestination > 0), takes the maximum value.
        # It will only return the default 150 and abandon the fare if timeToDestination <= 0
        if costSample is not None and costSample > 0:
            return costSample
        else:
            return 150

    # ----------------------------------------------------------------------------------------------------------------
    def _allocateFare(self, origin, destination, time):
        bidders = self._fareBoard[origin][destination][time].bidders
        fareNode = self._parent.getNode(origin[0], origin[1])
        # initial dictionary for the first 4 fares
        initialFares = {}
        # dictionary that holds th fares once every taxi has taken a fare
        constraintFares = {}
        allocatedTaxi = -1

        if len(self._taxis) > 0 and len(self._fareBoard) > 0:
            if fareNode is not None:

                # attempt to stop taxis from getting stuck
                for taxi in bidders:
                    bidderNode = self._parent.getNode(self._taxis[taxi].currentLocation[0],
                                                      self._taxis[taxi].currentLocation[1])
                    travelToOrigin = self._parent.travelTime(bidderNode, fareNode)
                    travelToDestination = self._parent.travelTime(bidderNode,
                                                                  self._parent.getNode(destination[0], destination[1]))
                    for t in self._taxis:
                        if len(t._path) > 0 and len(t._path) == travelToOrigin and origin == t._path[-1]:
                            fareNode = None
                        if len(t._path) > 0 and len(t._path) == (
                                travelToOrigin + travelToDestination) and destination == t._path[-1]:
                            fareNode = None

                # if there is more than one bidder, we perform certain checks to fairly decide who to allocate the
                # fare to
                if len(bidders) > 1:
                    # start by taking all travel distances and putting them into dictionaries
                    for i in bidders:
                        bidderNode = self._parent.getNode(self._taxis[i].currentLocation[0],
                                                          self._taxis[i].currentLocation[1])
                        # if the taxi is already in the fare amount constraint dict, add it to constraintFares dict
                        # otherwise it means the taxi hasn't taken a fare which means, add it to initialFares dict
                        if i in self.fareAmountConstraint:
                            constraintFares[i] = self._parent.travelTime(bidderNode, fareNode)
                        else:
                            initialFares[i] = self._parent.travelTime(bidderNode, fareNode)

                    # if there are still taxis who haven't taken a bid, give it to the taxi with the lowest travel
                    # distance. This is only used for the first fare of each taxi
                    if initialFares:
                        allocatedTaxi = min(initialFares, key=initialFares.get)
                    else:
                        # minFareAmount will get the lowest fare count of all the taxis in the constraint
                        # lowestFares will simply check for duplicate taxis with the same low value (minFareAmount)
                        # validBidders will compare the lowestFares taxis and make sure they correspond to any
                        # of the bidders in the constraintFares.
                        minFareAmount = min(self.fareAmountConstraint.values())
                        lowestFares = [i for i, j in self.fareAmountConstraint.items() if
                                       j == minFareAmount]
                        validBidders = [x for x in constraintFares if x in lowestFares]
                        # if there is more than one valid taxi, we choose the one with the shortest travel time
                        if len(validBidders) > 1:
                            travelTimes = {}
                            for i in validBidders:
                                bidderNode = self._parent.getNode(self._taxis[i].currentLocation[0],
                                                                  self._taxis[i].currentLocation[1])
                                travelTimes[i] = self._parent.travelTime(bidderNode, fareNode)
                            allocatedTaxi = min(travelTimes, key=travelTimes.get)
                        else:
                            # if there is only one, we choose that taxi.
                            if len(validBidders) > 0:
                                allocatedTaxi = validBidders[0]
                            else:
                                # If there is no valid bidders, we take the taxi with the lowest fare amount from
                                # the constraintFares bidders.
                                allocatedTaxi = min(constraintFares, key=constraintFares.get)
                else:
                    # if there is only one bidder at all, give him the fare
                    allocatedTaxi = bidders[0]

                # once all the constraints are checked and a decision is made, allocate a fare to the taxi
                if allocatedTaxi >= 0:
                    self._fareBoard[origin][destination][time].taxi = allocatedTaxi
                    self._parent.allocateFare(origin, self._taxis[allocatedTaxi])
                    # updates the fare amount constraint counters every time a taxi takes a new fare.
                    # This will keep track of how many fares taxis have taken
                    if allocatedTaxi in self.fareAmountConstraint:
                        self.fareAmountConstraint[allocatedTaxi] += 1
                    else:
                        self.fareAmountConstraint[allocatedTaxi] = 1

    # function that simply prints all revenues
    def displayRevenues(self):
        totalRevenue = 0
        print(f"Current dispater revenue: {self._revenue}")
        totalRevenue += self._revenue
        for taxi in self._taxis:
            print(f"Taxi {taxi.number} current revenue: {taxi._revenue}")
            totalRevenue += taxi._revenue
        print(f"Total overall revenue is {totalRevenue}")
