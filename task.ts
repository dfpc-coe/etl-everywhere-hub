import Err from '@openaddresses/batch-error';
import { Feature } from '@tak-ps/node-cot';
import Schema from '@openaddresses/batch-schema';
import type { Static, TSchema } from '@sinclair/typebox';
import { Type } from '@sinclair/typebox';
import ETL, { DataFlowType, SchemaType, handler as internal, local, InvocationType, fetch } from '@tak-ps/etl';
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

const DebugItem = Type.Unsafe<Static<typeof EverywhereItem>>(Type.Any());

const EphemeralStore = Type.Object({
    cachetime: Type.Optional(Type.Integer({
        description: 'Cache timestamp for the last time devices were synced via the API'
    })),
    devices: Type.Optional(Type.Record(Type.String(), Feature.InputFeature))
})

const Input = Type.Object({
    TokenId: Type.Optional(Type.String({
        description: 'Everywhere Hub Token ID that can be used to optionally resync cache on a schedule'
    })),
    CacheRefresh: Type.Integer({
        // 5 minutes
        default: 300000,
        description: 'How often to perform a data refresh with the Everywhere Hub API if a TokenId is provided'
    }),
    RetentionDuration: Type.Integer({
        default: 3600 * 1000,
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
        if (flow !== DataFlowType.Incoming) return Type.Object({});

        if (type === SchemaType.Input) return Input;

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
        });
    }

    static async webhooks(
        schema: Schema,
        task: ETL
    ): Promise<void> {
        const env = await task.env(Input);

        await schema.post('/:webhookid', {
            name: 'Incoming Webhook',
            group: 'Default',
            description: 'Get an Everywhere Hub InReach Update',
            params: Type.Object({
                webhookid: Type.String()
            }),
            body: env.DEBUG ? DebugItem : EverywhereItem,
            res: Type.Object({
                status: Type.Number(),
                message: Type.String()
            })
        }, (req, res) => {
            if (env.DEBUG) {
                console.error(`DEBUG Webhook: ${req.params.webhookid} - ${JSON.stringify(req.body, null, 4)}`);
            }

            Task.receive(task, req.body)
                .then(() => res.json({
                    status: 200,
                    message: 'Received'
                }))
                .catch((err: unknown) => {
                    Err.respond(err, res);
                });
        })
    }

    static async receive(
        task: ETL,
        body: Static<typeof EverywhereItem>
    ): Promise<void> {
        const feat: Static<typeof Feature.InputFeature> = {
            id: `inreach-${body.entityId}`,
            type: 'Feature',
            properties: {
                type: body.trackPoint.isEmergency ? 'b-a-o-tbl' : 'a-f-G-U-U-S-X',
                how: 'm-g',
                course: body.trackPoint.direction,
                callsign: body.alias || body.name,
                time: new Date(body.trackPoint.time).toISOString(),
                start: new Date(body.trackPoint.time).toISOString(),
                metadata: {
                    inreachId: body.entityId,
                    inreachName: body.name,
                    inreachDeviceType: body.deviceType,
                    inreachDeviceId: body.deviceId,
                    inreachReceive: new Date(body.trackPoint.time).toISOString()
                }
            },
            geometry: {
                type: 'Point',
                coordinates: [ body.trackPoint.point.x, body.trackPoint.point.y ]
            }
        }

        const ephem = await task.ephemeral(EphemeralStore, DataFlowType.Incoming);
        ephem.devices ??= {};

        ephem.devices[`inreach-${body.entityId}`] = feat;
        await task.setEphemeral(ephem)

        await task.submit({
            type: 'FeatureCollection',
            features: [feat]
        });
    }

    async control(): Promise<void> {
        const env = await this.env(Input);

        const ephem = await this.ephemeral(EphemeralStore, DataFlowType.Incoming);
        ephem.devices ??= {};

        if (env.TokenId && (ephem.cachetime === undefined || ephem.cachetime === 0 || ephem.cachetime < Date.now() - env.CacheRefresh)) {
            console.log('ok - refreshing cache from Everywhere Hub API');

            const url = new URL('https://everywhere-hub.com/v2/api/tracks')
            url.searchParams.set('tokenId', env.TokenId);
            url.searchParams.set('noEarlierThan', String(Date.now() - env.RetentionDuration));
            url.searchParams.set('latestPositionOnly', String(true));

            const res = await fetch(url);

            const latest = await res.typed(Type.Object({
                type: Type.Literal('FeatureCollection'),
                features: Type.Array(Type.Object({
                    type: Type.Literal('Feature'),
                    properties: Type.Object({
                        name: Type.String(),
                        entityId: Type.Integer(),
                        entityType: Type.String(),
                        deviceType: Type.String(),
                        alias: Type.Optional(Type.String()),
                        oemSerial: Type.Optional(Type.String()),
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

            ephem.cachetime = Date.now();

            const fc: Static<typeof Feature.InputFeatureCollection> = {
                type: 'FeatureCollection',
                features: latest.features.map((feature) => {
                    return {
                        id: `inreach-${feature.properties.entityId}`,
                        type: 'Feature',
                        properties: {
                            type: feature.properties.isEmergency ? 'b-a-o-tbl' : 'a-f-G-U-U-S-X',
                            how: 'm-g',
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

            ephem.devices = {};
            for (const feat of fc.features) {
                ephem.devices[String(feat.id)] = feat;
            }
        } else {
            console.log('ok - reusing cached data');

            const fc: Static<typeof Feature.InputFeatureCollection> = {
                type: 'FeatureCollection',
                features: []
            };

            for (const [k, v] of Object.entries(ephem.devices)) {
                if (new Date(v.properties.time ?? 0).getTime() < Date.now() - env.RetentionDuration) {
                    delete ephem.devices[k];
                    continue;
                }

                fc.features.push(v);
            }

            await this.submit(fc);
        }

        await this.setEphemeral(ephem)
    }
}

await local(await Task.init(import.meta.url), import.meta.url);

export async function handler(event: Event = {}, context?: object): ReturnType<typeof internal> {
    return internal(await Task.init(import.meta.url, {
        logging: {
            event: Boolean(process.env.DEBUG),
            webhooks: true
        }
    }), event, context);
}
