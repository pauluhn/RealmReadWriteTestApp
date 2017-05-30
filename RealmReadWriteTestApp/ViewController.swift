//
//  ViewController.swift
//  RealmReadWriteTestApp
//
//  Created by Paul Uhn on 5/25/17.
//  Copyright © 2017 Paul Uhn. All rights reserved.
//

import UIKit
import RealmSwift

class DemoObjectChild: Object {
    dynamic var  id = ""
    dynamic var title = ""
    dynamic var date = Date()
    override public static func primaryKey() -> String? { return "id" }
    convenience init (id: String, title: String, date: Date) {
        self.init()
        self.id = id
        self.title = title
        self.date = date
    }
}


class DemoObject: Object {
    dynamic var  id = ""
    dynamic var title = ""
    dynamic var date = Date()
    dynamic var groupId = ""
    public let childs = List<DemoObjectChild>()
    override public static func primaryKey() -> String? { return "id" }
    convenience init (id: String, title: String, date: Date, groupId: String) {
        self.init()
        self.id = id
        self.title = title
        self.date = date
        self.groupId = groupId
    }
}

var baseURL: URL {
    return try! FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: false)
}
enum RealmType: String {
    case writeOnly
    case read
    case listener
    case twoListeners
    case threadListener
    var fileURL: URL {
        return baseURL.appendingPathComponent(self.rawValue).appendingPathExtension("realm")
    }
    var fileSize: UInt64 {
        return try! FileManager.default.attributesOfItem(atPath: fileURL.path)[.size] as! UInt64
    }
}
func realmOfType(_ type: RealmType) -> Realm {
    return try! Realm(configuration: Realm.Configuration(fileURL: type.fileURL))
}
extension Realm {
    var results: Results<DemoObject> {
        return objects(DemoObject.self)
    }
}
extension UInt64 {
    var mb: String {
        return String(format: "%.2f mb", Double(self) / 1024.0 / 1024.0)
    }
}
class ViewController: UIViewController {
    var tokens: [NotificationToken] = []
    var notificationTokenThread: NotificationToken?
    var fetchThread: Thread?
    var fetchThreadRunLoop: CFRunLoop?
    var fetchThreadExitSignal: NSCondition = NSCondition()
    var fetchResultsThread: Results<DemoObject>?
    var fetchThreadShouldStop = false
    var fetchThreadRealm: Realm?
    var notificationsReceivedOnThread = 0
    let kNumberOfBackgroundQueues = 16
    let kNumberOfObjects = 10000
    
    var readCount = 0
    var listenerCount = 0
    var twoListenersCount = 0
    var threadListenerCount = 0
    var writeOnlySize: UInt64 = 0 {
        didSet {
            writeOnlyLabel.text = "write only: \(writeOnlySize.mb)"
        }
    }
    var readSize: UInt64 = 0 {
        didSet {
            readLabel.text = "read: \(readSize.mb)\n\(readCount)"
        }
    }
    var listenerSize: UInt64 = 0 {
        didSet {
            listenerLabel.text = "listener: \(listenerSize.mb)\n\(listenerCount)"
        }
    }
    var twoListenersSize: UInt64 = 0 {
        didSet {
            twoListenersLabel.text = "two listeners: \(twoListenersSize.mb)\n\(twoListenersCount)"
        }
    }
    var threadListenerSize: UInt64 = 0 {
        didSet {
            threadListenerLabel.text = "thread listener: \(threadListenerSize.mb)\n\(threadListenerCount)"
        }
    }

    @IBOutlet weak var writeOnlyLabel: UILabel!
    @IBOutlet weak var readLabel: UILabel!
    @IBOutlet weak var listenerLabel: UILabel!
    @IBOutlet weak var twoListenersLabel: UILabel!
    @IBOutlet weak var threadListenerLabel: UILabel!

    override func viewDidLoad() {
        super.viewDidLoad()
        
        // delete realm files
        print(baseURL)
        try! FileManager.default.removeItem(at: baseURL)
        try! FileManager.default.createDirectory(at: baseURL, withIntermediateDirectories: false, attributes: nil)
        
        let results = realmOfType(.listener).results
        tokens.append(results.addNotificationBlock { _ in
            DispatchQueue.main.async {
                self.listenerCount = results.count
                self.listenerSize = RealmType.listener.fileSize
            }
        })
        let results2 = realmOfType(.twoListeners).results
        tokens.append(results2.addNotificationBlock { _ in
            DispatchQueue.main.async {
                self.twoListenersCount = results2.count
                self.twoListenersSize = RealmType.twoListeners.fileSize
            }
        })
        let results3 = realmOfType(.twoListeners).results
        tokens.append(results3.addNotificationBlock { _ in
            DispatchQueue.main.async {
                self.twoListenersCount = results3.count
                self.twoListenersSize = RealmType.twoListeners.fileSize
            }
        })
    }
    
    override func viewDidAppear(_ animated: Bool) {
        super.viewDidAppear(animated)
        // start write transactions in backround
        for i in 0...kNumberOfBackgroundQueues {
            self.writeOnBackground(queueIndex: i, realmType: .writeOnly)
            self.writeOnBackground(queueIndex: i, realmType: .read)
            self.writeOnBackground(queueIndex: i, realmType: .listener)
            self.writeOnBackground(queueIndex: i, realmType: .twoListeners)
            self.writeOnBackground(queueIndex: i, realmType: .threadListener)
        }
        // start fetch thread
        self.startFetchThread()
    }
    
    // write transactions on background queues
    func writeOnBackground(queueIndex: Int, realmType: RealmType) {
        // delay between backround writes on particular queue: 0.05 - 0.1 seconds
        let delay = Double((arc4random_uniform(50) + 50))/1000.0
        DispatchQueue.global(qos: .userInitiated).asyncAfter(deadline: .now() + delay) {
            autoreleasepool {
                // 0...3 - add new
                // 4 - update
                // 5...8 - add chiled
                let action = arc4random_uniform(9)
                
                if realmType == .read {
                    self.doFetch()
                }
                
                if action < 4 {
                    self.doAdd(queueIndex: queueIndex, realmType: realmType)
                } else if action >= 5 {
                    self.doAddChild(queueIndex: queueIndex, realmType: realmType)
                } else {
                    self.doUpdate(queueIndex: queueIndex, realmType: realmType)
                }
            }
            // schedule next write
            self.writeOnBackground(queueIndex: queueIndex, realmType: realmType)
            if realmType == .writeOnly {
                DispatchQueue.main.async {
                    self.writeOnlySize = RealmType.writeOnly.fileSize
                }
            }
        }
    }
    
    func doAdd(queueIndex: Int, realmType: RealmType) {
        let createId = queueIndex
        let realm = realmOfType(realmType)
        try! realm.write {
            if realmType == .read {
                self.doFetch()
            }
            let uuid = UUID().uuidString
            let newObject = DemoObject(id: "\(createId)-\(uuid)", title: uuid, date: Date(), groupId: "\(createId)")
            realm.add(newObject, update: true)
        }
    }
    
    func doUpdate(queueIndex: Int, realmType: RealmType) {
        // updating random object
        var updateId = queueIndex - 1
        if updateId < 0 {
            updateId += self.kNumberOfBackgroundQueues
        }
        let realm = realmOfType(realmType)
        let objectsToUpdate = realm.results.filter(NSPredicate(format: "groupId = %@", "\(updateId)"))
        let count = objectsToUpdate.count
        guard count > 0 else { return }
        let index = arc4random_uniform(UInt32(count))
        let objectToUpdate = objectsToUpdate[Int(index)]
        try! realm.write {
            if realmType == .read {
                self.doFetch()
            }
            objectToUpdate.date = Date()
        }
    }
    
    func doAddChild(queueIndex: Int, realmType: RealmType) {
        // adding new child to random object
        var childId = queueIndex - 2
        if childId < 0 {
            childId += self.kNumberOfBackgroundQueues
        }
        
        let realm = realmOfType(realmType)
        let objectsToAddChild = realm.results.filter(NSPredicate(format: "groupId = %@", "\(childId)"))
        let count = objectsToAddChild.count
        guard count > 0 else { return }
        let index = arc4random_uniform(UInt32(count))
        let objectToAddChild = objectsToAddChild[Int(index)]
        try! realm.write {
            if realmType == .read {
                self.doFetch()
            }
            let uuid = UUID().uuidString
            let newObjectChild = DemoObjectChild(id: "\(childId)-\(uuid)", title: uuid, date: Date())
            realm.add(newObjectChild, update: true)
            objectToAddChild.childs.append(newObjectChild)
        }
    }
    
    func doFetch() {
        let count = realmOfType(.read).results.count
        DispatchQueue.main.async {
            self.readCount = count
            self.readSize = RealmType.read.fileSize
        }
    }
    
    // fetch thread procedure
    func fetchThreadProc() {
        autoreleasepool {
            self.notificationsReceivedOnThread = 0
            self.fetchThreadRunLoop = CFRunLoopGetCurrent()
            self.fetchThreadRealm = realmOfType(.threadListener)
            
            CFRunLoopPerformBlock(self.fetchThreadRunLoop, CFRunLoopMode.defaultMode.rawValue) {
                // fetch all objects
                autoreleasepool {
                    self.fetchThreadRealm?.refresh()
                    self.fetchResultsThread = self.fetchThreadRealm?.results
                    
                    // observer notifications
                    self.notificationTokenThread?.stop()
                    self.notificationTokenThread = nil
                    self.notificationTokenThread = self.fetchResultsThread?.addNotificationBlock { [weak self] _ in
                        let count = self?.fetchResultsThread?.count ?? 0
                        self?.notificationsReceivedOnThread += 1
                        DispatchQueue.main.async {
                            self?.threadListenerCount = count
                            self?.threadListenerSize = RealmType.threadListener.fileSize
                        }
                    }
                }
            }
            
            while (!self.fetchThreadShouldStop) {
                CFRunLoopRunInMode(CFRunLoopMode.defaultMode, 1.0, true)
            }
            
            print("\(Date().timeIntervalSinceReferenceDate): DEBUG: total received = \(self.notificationsReceivedOnThread)")
            
            self.fetchThreadShouldStop = false
            self.notificationTokenThread?.stop()
            self.notificationTokenThread = nil
            self.fetchThreadRunLoop  = nil
            
            // !!! without this notifications will stop firing after several thread start/stop cycles
            //self.fetchResultsThread = nil
            
            self.fetchThreadExitSignal.lock()
            self.fetchThreadExitSignal.signal()
            self.fetchThreadExitSignal.unlock()
        }
    }
    
    func startFetchThread() {
        guard self.fetchThread == nil else {
            return
        }
        
        DispatchQueue.main.async {
            // wait until thread comes to a complete stop
            if self.fetchThreadRunLoop != nil {
                self.fetchThreadExitSignal.lock()
                self.fetchThreadExitSignal.wait()
                self.fetchThreadExitSignal.unlock()
            }
            self.fetchThread = Thread(target: self, selector: #selector(self.fetchThreadProc), object: nil)
            self.fetchThread?.start()
            
            // schedule stop after 5 to 15 seconds
            let stopAfter = Double((arc4random_uniform(100) + 50))/10.0
            print("\(Date().timeIntervalSinceReferenceDate): DEBUG: thread is about to start; stop scheduled after: \(stopAfter)");
            DispatchQueue.main.asyncAfter(deadline: .now() + stopAfter) {
                self.stopFetchThread()
            }
        }
        
    }
    
    func stopFetchThread() {
        guard self.fetchThreadRunLoop != nil else {
            return
        }
        
        self.fetchThreadShouldStop = true
        self.fetchThread = nil;
        
        // schedule start after 0.1 to 1 seconds
        let startAfter = Double(arc4random_uniform(10) + 1) / 10.0
        print("\(Date().timeIntervalSinceReferenceDate): DEBUG: thread is about to stop; start scheduled after: \(startAfter)");
        DispatchQueue.main.asyncAfter(deadline: .now() + startAfter) {
            self.startFetchThread()
        }
    }
}
