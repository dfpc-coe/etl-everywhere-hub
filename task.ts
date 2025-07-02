import Err from '@openaddresses/batch-error';
import Schema from '@openaddresses/batch-schema';
import type { Static, TSchema } from '@sinclair/typebox';
import { Type } from '@sinclair/typebox';
import ETL, { DataFlowType, SchemaType, handler as internal, local, InvocationType, fetch, InputFeatureCollection, InputFeature } from '@tak-ps/etl';
import type { Event } from '@tak-ps/etl';

export interface Share {
    ShareId: string;
    CallSign?: string;
    Password?: string;
}

const EverywhereItem = Type.Object({
    converterId: Type.String(),
    deviceId: Type.Integer(),
    teamId: Type.Integer(),
    trackPoint: Type.Object({
        time: Type.Integer(),
        direction: Type.Integer(),
        inboundMessageId: Type.Integer(),
        isEmergency: Type.Optional(Type.Boolean()),
        source: Type.Optional(Type.String()),
        alertsList: Type.Optional(Type.Array(Type.Object({
            id: Type.Integer(),
            description: Type.String(),
            type: Type.String()
        }))),
        point: Type.Object({
            x: Type.Number(),
            y: Type.Number()
        }),
    }),
    source: Type.String(),
    entityId: Type.Integer(),
    deviceType: Type.String(),
    name: Type.String(),
    alias: Type.Optional(Type.String())
})

const EphemeralStore = Type.Object({
    cachetime: Type.Optional(Type.Integer({
        description: 'Cache timestamp for the last time devices were synced via the API'
    })),
    devices: Type.Optional(Type.Record(Type.String(), InputFeature))
})

const Input = Type.Object({
    TokenId: Type.Optional(Type.String({
        description: 'Everywhere Hub Token ID that can be used to optionally resync cache on a schedule'
    })),
    CacheRefresh: Type.Integer({
        default: 300000, // 5 minutes
        description: 'How often to perform a data refresh with the Everywhere Hub API if a TokenId is provided'
    }),
    RetentionDuration: Type.Integer({
        default: 3600 * 1000, // 30 minutes
        description: 'How long to retain data in milliseconds, defaults to 60 minutes'
    }),
    'DEBUG': Type.Boolean({
        default: false,
        description: 'Print debug info in logs'
    })
})

export default class Task extends ETL {
    static name = 'etl-everywhere-hub'
    static flow = [ DataFlowType.Incoming ];
    static invocation = [ InvocationType.Webhook, InvocationType.Schedule ];

    async schema(
        type: SchemaType = SchemaType.Input,
        flow: DataFlowType = DataFlowType.Incoming
    ): Promise<TSchema> {
        if (flow === DataFlowType.Incoming) {
            if (type === SchemaType.Input) {
                return Input;
            } else {
                return Type.Object({
                    inreachId: Type.String(),
                    inreachName: Type.String(),
                    inreachDeviceType: Type.String(),
                    inreachIMEI: Type.Optional(Type.String()),
                    inreachIncidentId: Type.Optional(Type.String()),
                    inreachValidFix: Type.Optional(Type.String()),
                    inreachText: Type.Optional(Type.String()),
                    inreachEvent: Type.Optional(Type.String()),
                    inreachDeviceId: Type.String(),
                    inreachReceive: Type.String({ format: 'date-time' }),
                })
            }
        } else {
            return Type.Object({});
        }
    }

    static async webhooks(
        schema: Schema,
        task: Task
    ): Promise<void> {
        const env = await task.env(Input);

        schema.post('/:webhookid', {
            name: 'Incoming Webhook',
            group: 'Default',
            description: 'Get an Everywhere Hub InReach Update',
            params: Type.Object({
                webhookid: Type.String()
            }),
            body: env.DEBUG ? Type.Any() : EverywhereItem,
            res: Type.Object({
                status: Type.Number(),
                message: Type.String()
            })
        }, async (req, res) => {
            if (env.DEBUG) {
                console.error(`DEBUG Webhook: ${req.params.webhookid} - ${JSON.stringify(req.body, null, 4)}`);
            }

            try {
                const ephem = await task.ephemeral(EphemeralStore, DataFlowType.Incoming);
                if (ephem.devices) ephem.devices = {};

                ephem.devices[`inreach-${req.body.entityId}`] = req.body;
                await task.setEphemeral(ephem)

                const feat: Static<typeof InputFeature> = {
                    id: `inreach-${req.body.entityId}`,
                    type: 'Feature',
                    properties: {
                        course: req.body.trackPoint.direction,
                        callsign: req.body.alias || req.body.name,
                        time: new Date(req.body.trackPoint.time).toISOString(),
                        start: new Date(req.body.trackPoint.time).toISOString(),
                        metadata: {
                            inreachId: req.body.entityId,
                            inreachName: req.body.name,
                            inreachDeviceType: req.body.deviceType,
                            inreachDeviceId: req.body.deviceId,
                            inreachReceive: new Date(req.body.trackPoint.time).toISOString()
                        }
                    },
                    geometry: {
                        type: 'Point',
                        coordinates: [ req.body.trackPoint.point.x, req.body.trackPoint.point.y ]
                    }
                }

                await task.submit({
                    type: 'FeatureCollection',
                    features: [feat]
                });

                res.json({
                    status: 200,
                    message: 'Received'
                });
            } catch (err) {
                Err.respond(err, res);
            }
        })
    }

    async control(): Promise<void> {
        const env = await this.env(Input);
        const ephem = await this.ephemeral(EphemeralStore, DataFlowType.Incoming);

        if (env.TokenId && (!ephem.cachetime || ephem.cachetime < new Date().getTime() - env.CacheRefresh)) {
            const url = new URL('https://everywhere-hub.com/v2/api/tracks')
            url.searchParams.set('tokenId', env.TokenId);
            url.searchParams.set('noEarlierThan', String(new Date().getTime() - env.RetentionDuration));
            url.searchParams.set('latestPositionOnly', String(true));

            const res = await fetch(url);

            const latest = await res.typed(Type.Object({
                type: Type.Literal('FeatureCollection'),
                features: Type.Array(Type.Object({
                    id: Type.String(),
                    type: Type.Literal('Feature'),
                    properties: Type.Object({
                        name: Type.String(),
                        entityId: Type.Integer(),
                        entityType: Type.String(),
                        deviceType: Type.String(),
                        alias: Type.String(),
                        oemSerial: Type.String(),
                        teamId: Type.Integer(),
                        time: Type.Integer(),
                        inboundMessageId: Type.Integer(),
                        isEmergency: Type.Optional(Type.Boolean()),
                        direction: Type.Number(),
                        source: Type.Optional(Type.String())
                    }),
                    geometry: Type.Object({
                        type: Type.Literal('Point'),
                        coordinates: Type.Array(Type.Number())
                    })
                }))
            }));

            ephem.cachetime = new Date().getTime();

            const fc: Static<typeof InputFeatureCollection> = {
                type: 'FeatureCollection',
                features: latest.features.map((feature) => {
                    return {
                        id: `inreach-${feature.properties.entityId}`,
                        type: 'Feature',
                        properties: {
                            course: feature.properties.direction,
                            callsign: feature.properties.alias || feature.properties.name,
                            time: new Date(feature.properties.time).toISOString(),
                            start: new Date(feature.properties.time).toISOString(),
                            metadata: {
                                inreachId: feature.properties.entityId,
                                inreachName: feature.properties.name,
                                inreachDeviceType: feature.properties.deviceType,
                                inreachDeviceId: 'UNKNOWN',
                                inreachReceive: new Date(feature.properties.time).toISOString()
                            }
                        },
                        geometry: feature.geometry
                    }
                })
            };

            await this.submit(fc);

            for (const feat of fc.features) {
                ephem.devices = {};
                ephem.devices[feat.id] = feat;
            }

            await this.setEphemeral(ephem)

        } else {
            const fc: Static<typeof InputFeatureCollection> = {
                type: 'FeatureCollection',
                features: []
            };

            for (const [k, v] of Object.entries(ephem.devices || {})) {
                if (new Date(v.properties.time).getTime() < new Date().getTime() - env.RetentionDuration) {
                    delete ephem.devices[k];
                    continue;
                }

                fc.features.push(v);
            }

            await this.setEphemeral(ephem)

            await this.submit(fc);
        }
    }
}

await local(await Task.init(import.meta.url), import.meta.url);

export async function handler(event: Event = {}, context?: object) {
    return await internal(await Task.init(import.meta.url, {
        logging: {
            event: process.env.DEBUG ? true : false,
            webhooks: true
        }
    }), event, context);
}
