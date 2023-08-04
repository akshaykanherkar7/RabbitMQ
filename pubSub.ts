import client, {Channel, Connection} from 'amqplib'
import logger from './logger/index'
import dayjs from 'dayjs'
import utc from 'dayjs/plugin/utc'
dayjs.extend(utc)

export const pubInit = async () => { await init(String(process.env.AMQP_BASE_URL)) }
export const pubSendMessage = async (customerId:number, eventKey:string, eventPayload:any) => { 
    return await sendMessage(customerId, eventKey, eventPayload) 
}
export const pubCloseConn = async () => { closeConnection() }
class QueueConnection {
    channel: Channel|undefined

    async init(url:string){
        const ZYPE_QUEUE_NAME:string = process.env.ZYPE_QUEUE_NAME || ''
        logger.info(`Initialising Queue conection with ${url}`)
        const connection: Connection = await client.connect(url)
        logger.info(`Conection created`)
        this.channel = await connection.createChannel()
        logger.info(`Channel created`)
        await this.channel.assertQueue(ZYPE_QUEUE_NAME)
        logger.info(`Asserting queue for ${ZYPE_QUEUE_NAME}`)
        return this.channel
    }

    async sendMessage(userId:number, eventKey:string, eventPayload?:any){
        //TODO: Check date format  
        const ZYPE_QUEUE_NAME:string = process.env.ZYPE_QUEUE_NAME || ''
        let currTime:string = dayjs.utc().format('YYYY-MM-DDTHH:mm:SS+0530');
        logger.info(`Current timestamp for ${eventKey} is ${currTime} for userid ${userId}`)
        if(this.channel){
            const messageObject:any = {}
            messageObject["userId"]=userId;
            messageObject["eventName"]=eventKey
            messageObject["eventData"]=eventPayload
            messageObject["eventTime"]=currTime
            logger.debug(`Message Object is ${JSON.stringify(messageObject)}`)
            this.channel.sendToQueue(ZYPE_QUEUE_NAME, Buffer.from(JSON.stringify(messageObject)))
            logger.debug(`Message Sent to ${ZYPE_QUEUE_NAME} for userid ${userId}`)
        }else{
            logger.error(`Message channel was not initialised`)
        }
    }

    close(){
        logger.debug(`Closing channel`)
        this.channel?.close()
    }
}

const conn:QueueConnection = new QueueConnection();

export const  sendMessage = async (userId:number, eventKey:string, eventPayload:any) => {
    await conn.sendMessage(userId, eventKey, eventPayload);
}
export const init = async(url:string)=>{
    await conn.init(url)
}
export const closeConnection = ()=> {
    conn.close()
}

